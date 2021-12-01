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

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TestWrite extends FunSuite {

  test("Test write a local file to openmldb") {
    val sess = SparkSession.builder().master("local[*]").getOrCreate()
    val df = sess.read.option("header", "true").schema("c1 bigint").csv("openmldb-spark-connector/src/test/resources/test.csv")
    df.show()
    val options = Map("db" -> "db", "table" -> "t1")
    // TODO(hw): smaller format string?
    // batch write can't use ErrorIfExists
    df.write
      //      .format("com._4paradigm.openmldb.spark.OpenmldbSource")
      .format("openmldb")
      .options(options).mode("append").save()
  }
}
