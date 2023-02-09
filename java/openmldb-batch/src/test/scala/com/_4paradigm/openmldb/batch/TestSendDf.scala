/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.batch

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType, StringType}

import scala.collection.JavaConverters.seqAsJavaListConverter
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.HttpHeaders
import org.apache.http.entity.StringEntity
import org.apache.commons.io.IOUtils
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang3.StringEscapeUtils

class TestSendDf extends SparkTestSuite {

  test("Test send df to taskmanager") {
    val sess = getSparkSession

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("time", TimestampType),
        StructField("amt", DoubleType),
        StructField("str", StringType)
      )
    )
    val data = (for (i <- 1 to 50) yield (i, Timestamp.valueOf("0001-01-01 0:0:0"), null, """a"b"c\n""")).toSeq

    val df = sess.createDataFrame(data.map(Row.fromTuple(_)).asJava, schema)
    df.show()
    var config = new OpenmldbBatchConfig
    config.saveJobResultHttp = "http://0.0.0.0:7129/openmldb.taskmanager.TaskManagerServer/SaveJobResult"
    config.saveJobResultId = "0"

    // rewrite later by sendResult
    // df.collect().foreach(x => println(x))
    println(df.rdd.getNumPartitions)

    val url = config.saveJobResultHttp
    val resultId = config.saveJobResultId
    val schemaLine = df.schema.map(structField => { structField.name }).mkString(",")
    println(s"hw test, trigger send result, ${url}, ${resultId}")
    df.foreachPartition { (partition: Iterator[Row]) =>
      {
        val client = HttpClientBuilder.create().build()
        val post = new HttpPost(url)
        while (partition.hasNext) {
          val arr = new ArrayBuffer[String]()
          var i = 0
          while (i < 100 && partition.hasNext) {
            // print, no need to do convert, but taskmanager should know it, one row is?
            arr.append(
              partition
                .next()
                .toSeq
                .map(x => {
                  if (x == null) { "null" }
                  else if(x.isInstanceOf[Timestamp]) { s""""${x.toString}"""" }
                  else if (x.isInstanceOf[String]) {
                    s""""${StringEscapeUtils.escapeJson(x.toString)}""""
                  } else { x.toString }
                })
                .mkString(",")
            )
            i += 1
          }
          // use json load to rebuild two dim array?
          // just send a raw file stream? "<schema>\m<row1>\n<row2>..."
          val data = StringEscapeUtils.escapeJson(arr.mkString("\n"))
          val json_str = s"""{"json_data": "${schemaLine}\\n${data}", "result_id": ${resultId}}"""
          println("hw test, json_str: " + json_str)
          post.setEntity(new StringEntity(json_str))
          post.setHeader("Content-type", "application/json");
          val response = client.execute(post)
          val entity = response.getEntity()
          println(Seq(response.getStatusLine.getStatusCode(), response.getStatusLine.getReasonPhrase()))
          println(IOUtils.toString(entity.getContent()))
        }
      }
    }

    // df.select(to_json(struct(col("*"))).as[String].foreachPartition(println)
    // send an empty data to finish all jobs
    val client = HttpClientBuilder.create().build()
    val post = new HttpPost(url)
    val json_str = s"""{"json_data": "", "result_id": ${resultId}}"""
    println("hw test, json_str: " + json_str)
    post.setHeader("Accept", "application/json");
    post.setHeader("Content-type", "application/json");
    post.setEntity(new StringEntity(json_str))
    val response = client.execute(post)
    val entity = response.getEntity()
    println(Seq(response.getStatusLine.getStatusCode(), response.getStatusLine.getReasonPhrase()))
    println(IOUtils.toString(entity.getContent()))
  }
}
