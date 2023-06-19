/*
 * // Copyright 2021-present StarRocks, Inc. All rights reserved.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     https://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package com.starrocks.connector.spark.sql;

import com.starrocks.connector.spark.cfg.PropertiesSettings;
import com.starrocks.connector.spark.rest.RestService;
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER;

public class SchemaITTest {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaITTest.class);

// StarRocks Table with json
//    CREATE TABLE `schema_test_with_json` (
//        c0 BOOLEAN,
//        c1 TINYINT,
//        c2 SMALLINT,
//        c3 INT,
//        c4 BIGINT,
//        c5 LARGEINT,
//        c6 FLOAT,
//        c7 DOUBLE,
//        c8 DECIMAL(20, 0),
//        c9 CHAR(10),
//        c10 VARCHAR(100),
//        c11 STRING,
//        c12 DATE,
//        c13 DATETIME
//    ) ENGINE=OLAP
//        PRIMARY KEY(`c0`, `c1`)
//        COMMENT "OLAP"
//        DISTRIBUTED BY HASH(`c0`)
//        PROPERTIES (
//        "replication_num" = "1"
//        );

// StarRocks Table without json
//    CREATE TABLE `schema_test_without_json` (
//        c0 BOOLEAN,
//        c1 TINYINT,
//        c2 SMALLINT,
//        c3 INT,
//        c4 BIGINT,
//        c5 LARGEINT,
//        c6 FLOAT,
//        c7 DOUBLE,
//        c8 DECIMAL(20, 0),
//        c9 CHAR(10),
//        c10 VARCHAR(100),
//        c11 STRING,
//        c12 DATE,
//        c13 DATETIME
//    ) ENGINE=OLAP
//        PRIMARY KEY(`c0`, `c1`)
//        COMMENT "OLAP"
//        DISTRIBUTED BY HASH(`c0`)
//        PROPERTIES (
//        "replication_num" = "1"
//        );


    private static final String FE_HTTP = "127.0.0.1:11901";
    private static final String FE_JDBC = "jdbc:mysql://127.0.0.1:11903";
    private static final String DB = "starrocks";
    private static final String TABLE_WITH_JSON = "schema_test_with_json";
    private static final String TABLE_ID_WITH_JSON = DB + "." + TABLE_WITH_JSON;
    private static final String TABLE_WITHOUT_JSON = "schema_test_without_json";
    private static final String TABLE_ID_WITHOUT_JSON = DB + "." + TABLE_WITHOUT_JSON;
    private static final String USER = "root";
    private static final String PASSWORD = "";

    @Test
    public void testStarRocksSchema() throws Exception {
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("starrocks.fenodes", FE_HTTP);
        settings.setProperty("starrocks.table.identifier", TABLE_ID_WITH_JSON);
        settings.setProperty(STARROCKS_REQUEST_AUTH_USER, USER);
        settings.setProperty(STARROCKS_REQUEST_AUTH_PASSWORD, PASSWORD);
        com.starrocks.connector.spark.rest.models.Schema restSchema = RestService.getSchema(settings, LOG);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID_WITH_JSON);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(options);
        StarRocksSchema jdbcSchema = StarRocksConnector.getSchema(config);
    }

    @Test
    public void testTime() {
        // output 0001-01-01 00:00:00.0
        System.out.println(Timestamp.valueOf("0000-01-01 00:00:00"));
        // output 0000-01-01T00:00:00Z
        System.out.println(Instant.parse("0000-01-01T00:00:00Z"));
        // output 0001-01-01
        System.out.println(Date.valueOf("0000-01-01"));
        // output 0000-01-01
        System.out.println(LocalDate.parse("0000-01-01"));
    }

    @Test
    public void testSparkSchema() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSparkSchema")
                .getOrCreate();

        Row row = RowFactory.create(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                "5",
                6.0f,
                7.0,
                BigDecimal.valueOf(8.0),
                "9",
                "10",
                "11",
                Date.valueOf("0000-01-01"),
                Timestamp.valueOf("0000-01-01 00:00:00"),
//                LocalDate.parse("0000-01-01"),
//                Instant.parse("0000-01-01T00:00:00Z"),
                "{\"key\": 1, \"value\": 2}"
            );

        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("c1", DataTypes.ByteType, false, Metadata.empty()),
                new StructField("c2", DataTypes.ShortType, false, Metadata.empty()),
                new StructField("c3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c4", DataTypes.LongType, false, Metadata.empty()),
                new StructField("c5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c6", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c7", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("c8", new DecimalType(20, 0), false, Metadata.empty()),
                new StructField("c9", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c10", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c11", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c12", DataTypes.DateType, false, Metadata.empty()),
                new StructField("c13", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("c14", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);

        df.write().format("console")
                .option("truncate", "false")
                .save();
    }

    @Test
    public void testReadWriteWithJson() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWrite")
                .getOrCreate();

        List<Row> data = new ArrayList<>();
        Row row = RowFactory.create(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                "5",
                6.0f,
                7.0,
                BigDecimal.valueOf(8.0),
                "9",
                "10",
                "11",
                Date.valueOf("0000-01-01"),
                Timestamp.valueOf("0000-01-01 00:00:00"),
                "{\"key\": 1, \"value\": 2}"
        );
        data.add(row);

        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("c1", DataTypes.ByteType, false, Metadata.empty()),
                new StructField("c2", DataTypes.ShortType, false, Metadata.empty()),
                new StructField("c3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c4", DataTypes.LongType, false, Metadata.empty()),
                new StructField("c5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c6", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c7", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("c8", new DecimalType(20, 0), false, Metadata.empty()),
                new StructField("c9", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c10", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c11", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c12", DataTypes.DateType, false, Metadata.empty()),
                new StructField("c13", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("c14", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID_WITH_JSON);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

//        TODO read does not support json currently
//        Dataset<Row> readDf = spark.read().format("starrocks")
//                .option("starrocks.table.identifier", TABLE_ID_WITH_JSON)
//                .option("starrocks.fenodes", FE_HTTP)
//                .option("user", USER)
//                .option("password", PASSWORD)
//                .load();
//        readDf.show(5);

        spark.stop();
    }

    @Test
    public void testReadWriteWithoutJson() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testWrite")
                .getOrCreate();

        List<Row> data = new ArrayList<>();
        Row row = RowFactory.create(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                "5",
                6.0f,
                7.0,
                BigDecimal.valueOf(8.0),
                "9",
                "10",
                "11",
                Date.valueOf("0000-01-01"),
                Timestamp.valueOf("0000-01-01 00:00:00")
        );
        data.add(row);

        StructType schema = new StructType(new StructField[]{
                new StructField("c0", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("c1", DataTypes.ByteType, false, Metadata.empty()),
                new StructField("c2", DataTypes.ShortType, false, Metadata.empty()),
                new StructField("c3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("c4", DataTypes.LongType, false, Metadata.empty()),
                new StructField("c5", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c6", DataTypes.FloatType, false, Metadata.empty()),
                new StructField("c7", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("c8", new DecimalType(20, 0), false, Metadata.empty()),
                new StructField("c9", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c10", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c11", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c12", DataTypes.DateType, false, Metadata.empty()),
                new StructField("c13", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fenodes", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID_WITHOUT_JSON);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        Dataset<Row> readDf = spark.read().format("starrocks")
                .option("starrocks.table.identifier", TABLE_ID_WITHOUT_JSON)
                .option("starrocks.fenodes", FE_HTTP)
                .option("user", USER)
                .option("password", PASSWORD)
                .load();
        readDf.show(5);

        spark.stop();
    }
}
