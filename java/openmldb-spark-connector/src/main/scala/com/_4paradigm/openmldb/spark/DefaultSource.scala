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

import com._4paradigm.openmldb.sdk.{Column, Schema}
import org.apache.hadoop.classification.{InterfaceAudience, InterfaceStability}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Data source for integration with Spark's [[DataFrame]] API.
 *
 * Serves as a factory for [[KuduRelation???]] instances for Spark. Spark will
 * automatically look for a [[RelationProvider]] implementation named
 * `DefaultSource` when the user specifies the path of a source during DDL
 * operations through [[org.apache.spark.sql.DataFrameReader.format]].
 *
 * No DataSourceV2 in spark 3.0.0, so we use DataSource
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class DefaultSource
  extends DataSourceRegister with RelationProvider with CreatableRelationProvider
    with SchemaRelationProvider with StreamSinkProvider {
  /**
   * A nice alias for the data source so that when specifying the format
   * "kudu" can be used in place of "com._4paradigm.openmldb.spark".
   * Note: This class is discovered by Spark via the entry in
   * `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`
   */
  override def shortName(): String = "openmldb"

  /**
   * Construct a BaseRelation using the provided context and parameters.
   *
   * @param sqlContext SparkSQL context
   * @param parameters parameters given to us from SparkSQL
   * @return a BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  /**
   * Creates a relation and inserts data to specified table.
   *
   * @param sqlContext
   * @param mode       Only Append mode is supported. It will upsert or insert data
   *                   to an existing table, depending on the upsert parameter
   * @param parameters Necessary parameters for openmldb.table, openldb.master, etc...
   * @param data       Dataframe to save into kudu
   * @return returns populated base relation
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???

  /**
   * Construct a BaseRelation using the provided context, parameters and schema.
   *
   * @param sqlContext SparkSQL context
   * @param parameters parameters given to us from SparkSQL
   * @param schema     the schema used to select columns for the relation
   * @return a BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = ???

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = ???
}

/**
 * Implementation of Spark BaseRelation.
 *
 * @param tableName     OpenMLDB table that we plan to read from
 * @param masterAddrs   OpenMLDB master addresses
 * @param operationType The default operation type to perform when writing to the relation
 * @param userSchema    A schema used to select columns for the relation
 * @param readOptions   OpenMLDB read options(unsupported)
 * @param writeOptions  OpenMLDB write options
 * @param sqlContext    SparkSQL context
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class OpenMLDBRelation(val dbName: String,
                       val tableName: String,
                       val zkAddress: String,
                       val zkPath: String,
                       /*val operationType: OperationType, for insert/delete*/
                       /*val userSchema: Option[StructType],*/
                       /*val readOptions: KuduReadOptions = new KuduReadOptions,*/
                       val writeOptions: OpenMLDBWriteOptions = new OpenMLDBWriteOptions)(val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation {
  val log: Logger = LoggerFactory.getLogger(getClass)

  private val context: OpenMLDBContext = new OpenMLDBContext(zkAddress, zkPath, sqlContext.sparkContext)

  private val tableSchema: Schema = context.client.getTableSchema(dbName, tableName)

  def sdkTypeToSparkType(col: Product with Serializable) = ???

  /**
   * Generates a SparkSQL schema from a OpenMLDB schema.
   *
   * @param kuduSchema the Kudu schema
   * @param fields     an optional column projection
   * @return the SparkSQL schema
   */
  def sparkSchema(openmldbSchema: Schema, fields: Option[Seq[String]] = None): StructType = {
    val colMap = openmldbSchema.getColumnList.asScala.map(c => {
      (c.getColumnName, c)
    }).toMap
    val activeCols: Seq[Column] = fields match {
      // TODO(hw):
      case Some(fieldNames) => fieldNames.map(colMap.get).map { col => col.getOrElse() }
      case None => openmldbSchema.getColumnList.asScala
    }
    val sparkColumns = activeCols.map { col =>
      val sparkType = sdkTypeToSparkType(col)
      StructField(col.getName, sparkType, col.isNullable)
    }
    StructType(sparkColumns)
  }

  /**
   * Generates a SparkSQL schema object so SparkSQL knows what is being
   * provided by this BaseRelation.
   *
   * @return schema generated from the Kudu table's schema
   */
  override def schema: StructType = {
    sparkSchema(tableSchema)
  }

  /**
   * Build the RDD to scan rows.
   *
   * @param requiredColumns columns that are being requested by the requesting query
   * @param filters         filters that are being applied by the requesting query
   * @return RDD will all the results from Kudu
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???

  /**
   * Writes data into an existing Kudu table.
   *
   * If the `kudu.operation` parameter is set, the data will use that operation
   * type. If the parameter is unset, the data will be upserted.
   *
   * @param data      [[DataFrame]] to be inserted into Kudu
   * @param overwrite must be false; otherwise, throws [[UnsupportedOperationException]]
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("overwrite is not yet supported")
    }
    context.writeRows(data, tableName, writeOptions)
  }

  /**
   * Returns the string representation of this OpenMLDBRelation
   *
   * @return OpenMLDB + tableName of the relation
   */
  override def toString: String = {
    "OpenMLDB " + this.tableName
  }
}

private[spark] object OpenMLDBRelation {
}

/**
 * Sinks provide at-least-once semantics by retrying failed batches,
 * and provide a `batchId` interface to implement exactly-once-semantics.
 * Since Kudu does not internally track batch IDs, this is ignored,
 * and it is up to the user to specify an appropriate `operationType` to achieve
 * the desired semantics when adding batches.
 *
 * The default `Upsert` allows for KuduSink to handle duplicate data and such retries.
 *
 * Insert ignore support (KUDU-1563) would be useful, but while that doesn't exist,
 * using Upsert will work. Delete ignore would also be useful.
 */
class OpenMLDBSink(val tableName: String,
                   val openMLDBAddrs: String,
                   val writeOptions: OpenMLDBWriteOptions)(val sqlContext: SQLContext)
  extends Sink {

  private val context: OpenMLDBContext =
    new OpenMLDBContext(openMLDBAddrs, sqlContext.sparkContext)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    context.writeRows(data, tableName, writeOptions)
  }
}
