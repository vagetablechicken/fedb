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
    val df = sess.read.option("header", "true")
      // openmldb table column type accepts bool, not boolean
      // spark timestampFormat is DateTime, so test.csv c9 can't be long int.
      .schema("c1 boolean, c2 smallint, c3 int, c4 bigint, c5 float, c6 double,c7 string, c8 date, c9 timestamp, c10_str string")
      .csv("openmldb-spark-connector/src/test/resources/test.csv")
    df.show()
    val options = Map("db" -> "db", "table" -> "t1", "zkCluster" -> "127.0.0.1:6181", "zkPath" -> "/onebox")

    // batch write can't use ErrorIfExists
    df.write
      //      .format("com._4paradigm.openmldb.spark.OpenmldbSource")
      .format("openmldb")
      .options(options).mode("append").save()
  }
}
