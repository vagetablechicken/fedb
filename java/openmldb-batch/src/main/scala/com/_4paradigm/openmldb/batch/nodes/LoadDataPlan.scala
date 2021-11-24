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
package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.vm.PhysicalLoadDataNode
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import org.apache.spark.sql.SparkSession

object LoadDataPlan {
  object FileFormat extends Enumeration {
    type FileFormat = Value
    val CSV, PARQUET = Value
  }

  def parseOptions(node: PhysicalLoadDataNode): (FileFormat.FileFormat, Map[String, String]) = {
    var format = FileFormat.CSV
    var options: Map[String, String] = Map()
    // header default: true
    options += ("header", "true")
    var hasHeader = true
    var nullValue = "null"
    var quote = "\0" // TODO(hw): ?
    var option = node.GetOption("format")
    if (option != null) {
      val f = option.GetStr()
      if (f.equalsIgnoreCase("parquet")) {
        format = FileFormat.PARQUET
      } else if (!f.equalsIgnoreCase("csv")) {
        throw new HybridSeException("file format unsupported")
      }
    }
    option = node.GetOption("delimiter")
    if (option != null) {
      options += ("sep", option.GetStr())
    }
    option = node.GetOption("header")
    if (option != null) {
      hasHeader = option.GetBool()
    }
    option = node.GetOption("null_value")
    if (option != null) {
      nullValue = option.GetStr()
    }
    option = node.GetOption("quote")
    if (option != null) {
      quote = option.GetStr()
    }

    (format, delimiter, hasHeader, nullValue, quote)
  }

  def gen(ctx: PlanContext, node: PhysicalLoadDataNode): SparkInstance = {

    val tableName = node.Table()
    val df = ctx.getDataFrame(tableName).getOrElse {
      throw new HybridSeException(s"Input table $tableName not found")
    }

    val inputFile = node.File()


    val spark = SparkSession
      .builder()
      .appName("LoadDataPlan")
      .getOrCreate()

    val format
    , options = parseOptions(node)
    // read input file
    val reader = spark.read.options(options)
    // TODO(hw): get offline address from nameserver
    val offlineAddress = "hdfs://"
    // write

    // TODO(hw): return ok?
    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), df)
  }
}
