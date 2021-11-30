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

package com._4paradigm.openmldb.spark.openmldb

import com._4paradigm.openmldb.SQLInsertRow
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.sdk.{SdkOption, SqlExecutor}
import org.apache.hadoop.classification.{InterfaceAudience, InterfaceStability}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.{Logger, LoggerFactory}

@InterfaceAudience.Public
@InterfaceStability.Evolving
class OpenMLDBContext(val zkAddress: String,
                      val zkPath: String,
                      sc: SparkContext)
//                   val socketReadTimeoutMs: Option[Long])
  extends Serializable {

  def appendToInsertRow(insertRow: SQLInsertRow, i: Int, value: Any): Unit = {
    // TODO(hw):
  }

  def buildInsertRow(row: Row, dbName: String, tableName: String): SQLInsertRow = {
    val insertRow = client.getInsertRow(dbName, tableName)
    insertRow.Init(10)
    for (i <- 0 until insertRow.GetSchema().GetColumnCnt()) {
      appendToInsertRow(insertRow, i, row.get(i))
    }
    insertRow
  }

  def writeRows(data: DataFrame, dbName: String, tableName: String, writeOptions: OpenMLDBWriteOptions): Unit = {
    data.foreach(row => {
      val insertRow = buildInsertRow(row, dbName, tableName)
      if (!client.executeInsert("", "", insertRow)) {
        throw new RuntimeException(s"insert failed, row: $insertRow")
      }
    })
  }

  val log: Logger = LoggerFactory.getLogger(getClass)
  lazy val client: SqlExecutor = {
    val option = new SdkOption
//    option.setZkCluster(zkAddress)
//    option.setZkPath(zkPath)
    option.setZkCluster("127.0.0.1:6181")
    option.setZkPath("/onebox")
    option.setSessionTimeout(200000) // TODO(hw): option?
    new SqlClusterExecutor(option)
  }

}
