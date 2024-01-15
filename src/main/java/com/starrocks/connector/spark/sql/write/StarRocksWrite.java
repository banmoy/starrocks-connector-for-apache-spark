// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.catalog.StarRocksCatalog;
import com.starrocks.connector.spark.catalog.StarRocksCatalogException;
import com.starrocks.connector.spark.catalog.StarRocksColumn;
import com.starrocks.connector.spark.catalog.StarRocksTable;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger log = LoggerFactory.getLogger(StarRocksWrite.class);

    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;
    private final StarRocksSchema starRocksSchema;
    private final boolean isBatch;
    private final TimeStat timeStat;

    @Nullable
    private transient String tempTable;

    public StarRocksWrite(LogicalWriteInfo logicalInfo, WriteStarRocksConfig config, StarRocksSchema starRocksSchema, boolean isBatch) {
        this.logicalInfo = logicalInfo;
        this.config = config;
        this.starRocksSchema = starRocksSchema;
        this.isBatch = isBatch;
        this.timeStat = new TimeStat();
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        timeStat.startPrepare = System.currentTimeMillis();
        WriteStarRocksConfig writeConfig = doPrepare();
        timeStat.endPrepare = System.currentTimeMillis();
        return new StarRocksWriterFactory(logicalInfo.schema(), writeConfig);
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        timeStat.startCommit = System.currentTimeMillis();
        doCommit(messages);
        timeStat.endCommit = System.currentTimeMillis();
        log.warn("batch query `{}` commit, {}", logicalInfo.queryId(), timeStat);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.warn("batch query `{}` abort", logicalInfo.queryId());
        doAbort(messages);
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(logicalInfo.schema(), config);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.warn("streaming query `{}` commit", logicalInfo.queryId());
        doCommit(messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.warn("streaming query `{}` abort", logicalInfo.queryId());
        doAbort(messages);
    }

    private boolean useStageMode() {
        return isBatch && starRocksSchema.isPrimaryKey()
                && config.isPartialUpdate()
                && config.isPartialUpdateColumnMode()
                && config.getWriteMode() == WriteStarRocksConfig.WriteMode.STAGE;
    }

    private WriteStarRocksConfig doPrepare() {
        if (!useStageMode()) {
            return config;
        }

        String tableName = String.format("tmp_%s_%s_%s",
                config.getDatabase(), config.getTable(), logicalInfo.queryId());
        StructType sparkSchema = logicalInfo.schema();
        StarRocksTable starRocksTable = new StarRocksTable.Builder()
                .setDatabaseName(config.getDatabase())
                .setTableName(tableName)
                .setTableType(StarRocksTable.TableType.DUPLICATE_KEY)
                .setTableKeys(
                        starRocksSchema.getPrimaryKeys().stream()
                                .map(StarRocksField::getName)
                                .collect(Collectors.toList())
                    )
                .setColumns(
                        Arrays.stream(sparkSchema.fields())
                                .map(f -> starRocksSchema.getField(f.name()))
                                .map(this::toStarRocksColumn)
                                .collect(Collectors.toList())
                    )
                .setComment(
                        String.format("Spark partial update with column mode, table: %s.%s, query: %s",
                                config.getDatabase(), config.getTable(), logicalInfo.queryId())
                    )
                .build();
        StarRocksCatalog catalog = new StarRocksCatalog(config.getFeJdbcUrl(), config.getUsername(), config.getPassword());
        try {
            catalog.createTable(starRocksTable, false);
        } catch (Exception e) {
            log.error("Failed to create temp table {}.{}", config.getDatabase(), tableName, e);
            throw e;
        }
        this.tempTable = tableName;
        return config.copy(config.getDatabase(), tempTable, Arrays.asList("partial_update", "partial_update_mode"));
    }

    private StarRocksColumn toStarRocksColumn(StarRocksField field) {
        return new StarRocksColumn.Builder()
                .setColumnName(field.getName())
                .setDataType(toStarRocksType(field))
                .setColumnSize(field.getSize() == null ? null : Integer.parseInt(field.getSize()))
                .setDecimalDigits(field.getScale() == null ? null : Integer.parseInt(field.getScale()))
                .setNullable(true)
                .build();
    }

    private String toStarRocksType(StarRocksField field) {
        String type = field.getType().toLowerCase(Locale.ROOT);
        switch (type) {
            case "tinyint":
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return field.getSize() == null ? "BOOLEAN" : "TINYINT";
            case "bigint unsigned":
                return "LARGEINT";
            case "smallint":
            case "int":
            case "bigint":
            case "float":
            case "double":
            case "decimal":
            case "char":
            case "varchar":
            case "json":
            case "date":
            case "datetime":
                return type.toUpperCase();
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported starrocks type, column name: %s, data type: %s", field.getName(), field.getType()));
        }
    }

    private void doCommit(WriterCommitMessage[] messages) {
        if (!useStageMode()) {
            return;
        }

        String srcTableId = String.format("`%s`.`%s`", config.getDatabase(), tempTable);
        String targetTableId = String.format("`%s`.`%s`", config.getDatabase(), config.getTable());
        List<String> primaryKeys = starRocksSchema.getPrimaryKeys().stream()
                .map(StarRocksField::getName)
                .collect(Collectors.toList());
        String joinedKeys = primaryKeys.stream()
                .map(key -> String.format("%s.`%s` = t2.`%s`", targetTableId, key, key))
                .collect(Collectors.joining(" AND "));
        String joinedColumns = Arrays.stream(config.getColumns())
                .filter(col -> !primaryKeys.contains(col))
                .map(col -> String.format("`%s` = `t2`.`%s`", col, col))
                .collect(Collectors.joining(", "));
        String updateSql = String.format("UPDATE %s SET %s FROM %s AS `t2` WHERE %s;",
                targetTableId, joinedColumns, srcTableId, joinedKeys);
        String setQueryTimeout = String.format("SET query_timeout = %s;", config.getUpdateTimeout());
        String setVar = "SET partial_update_mode = 'column';";
        log.warn("Update sql: {}", updateSql);
        try {
            getCatalog().executeUpdateStatement(setQueryTimeout, setVar, updateSql);
        } catch (Exception e) {
            log.error("Failed to execute update, temp table: {}, target table: {}", srcTableId, targetTableId, e);
            throw new StarRocksCatalogException("Failed to execute update for table " + targetTableId, e);
        }
        timeStat.endUpdate = System.currentTimeMillis();
        dropTempTable();
        log.warn("Success to execute update, temp table: {}, target table: {}", srcTableId, targetTableId);
    }

    private void doAbort(WriterCommitMessage[] messages) {
        if (!useStageMode()) {
            return;
        }
        dropTempTable();
    }

    private void dropTempTable() {
        if (tempTable != null) {
            try {
                getCatalog().dropTable(config.getDatabase(), tempTable, true);
            } catch (Exception e) {
                log.error("Failed to drop temp table {}.{}.", config.getDatabase(), tempTable, e);
            }
        }
    }

    private StarRocksCatalog getCatalog() {
        return new StarRocksCatalog(config.getFeJdbcUrl(), config.getUsername(), config.getPassword());
    }

    private static class TimeStat implements Serializable {
        long startPrepare;
        long endPrepare;
        long startCommit;
        long endUpdate;
        long endCommit;

        @Override
        public String toString() {
            long totalTimeMs = endCommit - startPrepare;
            long prepareTimeMs = endPrepare -startPrepare;
            long updateTimeMs = endUpdate - startCommit;
            long dropTableMs = endCommit - endUpdate;
            return "TimeStat{" +
                    "totalTimeMs=" + totalTimeMs +
                    ", prepareTimeMs=" + prepareTimeMs +
                    ", updateTimeMs=" + updateTimeMs +
                    ", dropTableMs=" + dropTableMs +
                    ", otherTimeMs=" + (totalTimeMs - prepareTimeMs - updateTimeMs - dropTableMs) +
                    '}';
        }
    }
}
