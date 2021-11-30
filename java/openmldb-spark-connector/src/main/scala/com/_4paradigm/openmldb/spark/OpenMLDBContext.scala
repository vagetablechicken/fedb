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

package com._4paradigm.openmldb.spark

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.sdk.{SdkOption, SqlExecutor}
import org.apache.hadoop.classification.{InterfaceAudience, InterfaceStability}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

@InterfaceAudience.Public
@InterfaceStability.Evolving
//@SerialVersionUID(1L)
class OpenMLDBContext(val zkAddress: String,
                      val zkPath: String,
                      sc: SparkContext)
//                   val socketReadTimeoutMs: Option[Long])
  extends Serializable {
  def writeRows(data: DataFrame, tableName: String, writeOptions: OpenMLDBWriteOptions): Unit = ???

  val log: Logger = LoggerFactory.getLogger(getClass)
  @transient lazy val client: SqlExecutor = {
    val option = new SdkOption
    option.setZkCluster(zkAddress)
    option.setZkPath(zkPath)
    option.setSessionTimeout(200000) // TODO(hw): option?
    new SqlClusterExecutor(option)
  }

}
