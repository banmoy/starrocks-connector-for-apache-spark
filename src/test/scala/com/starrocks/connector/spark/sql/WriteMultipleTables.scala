/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.spark.sql

import com.starrocks.data.load.stream.properties.{StreamLoadProperties, StreamLoadTableProperties}
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2
import com.starrocks.data.load.stream.{StreamLoadDataFormat, StreamLoadManager}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import scala.util.Random

object WriteMultipleTables {

  def createStreamLoadManager(
                               feHttpUrl: String,
                               feJdbcUrl: String,
                               userName: String,
                               password: String,
                               bufferSize: Long,
                               flushIntervalMs: Long): StreamLoadManager = {
    val properties: util.Map[String, String] = new util.HashMap[String, String]
    properties.put("format", "json")
    properties.put("strip_outer_array", "true")
    properties.put("ignore_json_size", "true")
    properties.put("timeout", "600")
    properties.put("compression", "lz4_frame")

    val tableProperties = StreamLoadTableProperties.builder
      .database("*")
      .table("*")
      .streamLoadDataFormat(StreamLoadDataFormat.JSON)
      .chunkLimit(20971520L)
      .maxBufferRows(Integer.MAX_VALUE)
      .addCommonProperties(properties)
      .build
    val streamLoadProperties = StreamLoadProperties.builder
      .defaultTableProperties(tableProperties)
      .loadUrls(feHttpUrl).jdbcUrl(feJdbcUrl)
      .username(userName)
      .password(password)
      .connectTimeout(180000)
      .waitForContinueTimeoutMs(180000)
      .ioThreadCount(20)
      .scanningFrequency(50)
      .cacheMaxBytes(bufferSize)
      .expectDelayTime(flushIntervalMs)
      .labelPrefix("spark")
      .addHeaders(properties)
      .enableTransaction()
      .maxRetries(0)
      .build()

    new StreamLoadManagerV2(streamLoadProperties, true)
  }

  def extractTargetTable(row: Row): String = {
    if (Random.nextInt(2) == 0) "rate1" else "rate2"
  }

  def transformToJson(row: Row): String = {
    val s = String.format("{\"ts\":\"%s\",\"id\":%s}", row.getTimestamp(0).toString, row.getLong(1).toString)
    System.out.println(s)
    s
  }

  /**
   * Assume there are two tables named test.rate1 and test.rate2. They have same
   * schema with 'CREATE TABLE test.rate (id int, ts DATETIME)'. Use 'rate' source
   * to generate records randomly, and ingest them to these two tables.
   */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("test multiple tables")
      .getOrCreate()

    val feHttpUrl = "http://127.0.0.1:8030"
    val feJdbcUrl = "jdbc://mysql/127.0.0.1:9030"
    val userName = "root"
    val password = ""
    val bufferSize = 104857600
    val flushIntervalMs = 300000

    // "rate" source generates increment long values with timestamps,  and
    // map them to the `id` and `ts` column of StarRocks table respectively.
    sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()
      .writeStream
      .foreachBatch((ds: Dataset[Row], batchId: Long) => {
        ds.rdd.foreachPartition(iterator => {
          val streamLoadManager = createStreamLoadManager(
            feHttpUrl, feJdbcUrl, userName, password, bufferSize, flushIntervalMs)
          try {
            streamLoadManager.init()
            for (row <- iterator) {
              val table = extractTargetTable(row)
              val json = transformToJson(row)
              streamLoadManager.write(null, "test", table, json)
            }
            streamLoadManager.flush()
          } finally {
            streamLoadManager.close()
          }
        })
      })
      .trigger(Trigger.ProcessingTime(1000))
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }
}
