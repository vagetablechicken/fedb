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

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object openmldb {

  /**
   * Adds a method, `kudu`, to DataFrameReader that allows you to read Kudu tables using
   * the DataFrameReader.
   */
  implicit class OpenMLDBDataFrameReader(reader: DataFrameReader) {

    @deprecated("Use `.format(\"openmldb\").load` instead", "1.9.0")
    def openmldb: DataFrame = reader.format("com._4paradigm.openmldb.spark.openmldb.spark").load
  }

  /**
   * Adds a method, `kudu`, to DataFrameWriter that allows writes to Kudu using
   * the DataFileWriter
   */
  implicit class OpenMLDBDataFrameWriter[T](writer: DataFrameWriter[T]) {

    @deprecated("Use `.format(\"openmldb\").save` instead", "1.9.0")
    def openmldb = writer.format("com._4paradigm.openmldb.spark.openmldb.spark").save
  }
}
