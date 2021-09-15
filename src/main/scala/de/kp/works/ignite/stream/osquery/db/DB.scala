package de.kp.works.ignite.stream.osquery.db
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.gson._
import de.kp.works.conf.WorksConf
import org.apache.ignite.Ignite
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.DataFrame

import java.util

/**
 * Osquery uses Apache Ignite as internal database
 * to manage configurations and nodes.
 */
object DB {

  private var instance:Option[DB] = None
  private var tables:Option[Boolean] = None

  def getInstance(ic:IgniteContext):DB = {

    if (ic == null) {
      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - No Ignite context available.")
    }
    if (instance.isEmpty)
      instance = Some(new DB(ic))
    /*
     * Build all Apache Ignite caches needed to manage
     * configurations, nodes, queries, and tasks
     */
    if (tables.isEmpty) {

      instance.get.buildTables()
      tables = Some(true)

    }

    instance.get

  }
}

class DB(ic:IgniteContext) {
  /*
   * SQL CREATE TABLE STATEMENTS
   */
  private val USING = "org.apache.spark.sql.redis"
  private val CREATE_TABLE = "create table if not exists %1(%2) using %3 options (table '%1')"

  private val ignite = ic.ignite()
  private val namespace = WorksConf.getNSCfg(WorksConf.OSQUERY_CONF)

  /** NODE **/

  def updateNode(values:Seq[Any]):Unit = {
    DBUtil.updateNode(ignite, namespace, values)
  }

  /** QUERY **/

  def createQuery(values:Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: description string
     * 3: sql string
     * 4: notbefore long
     */
    val sqlTpl = "insert into queries values ('%0', %1, '%2', '%3', %4)"
    insert(values, sqlTpl)

  }

  def updateQuery(values: Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: description string
     * 3: sql string
     * 4: notbefore long
     */
    val sqlTpl = "insert overwrite queries values ('%0', %1, '%2', '%3', %4)"
    insert(values, sqlTpl)

  }

  /** TASK **/

  def createTask(values:Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: query string
     * 4: status string
     */
    val sqlTpl = "insert into tasks values ('%0', %1, '%2', '%3', '%4')"
    insert(values, sqlTpl)

  }

  def updateTask(task:OsqueryQueryTask):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: query string
     * 4: status string
     */
    val values = Seq(
      task.uuid,
      task.timestamp,
      task.node,
      task.query,
      task.status
    ).map(_.toString)

    updateTask(values)

  }
  def updateTask(values:Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: query string
     * 4: status string
     */
    val sqlTpl = "insert overwrite tasks values ('%0', %1, '%2', '%3', '%4')"
    insert(values, sqlTpl)

  }

  /** CONFIGURATION **/

  def createConfiguration(values:Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: config string
     */
    val sqlTpl = "insert into configurations values ('%0', %1, '%2', '%3', '%4')"
    insert(values, sqlTpl)

  }

  def updateConfiguration(values:Seq[String]):Unit = {
    /*
     * It is expected that the values are in the same order
     * as the defined columns:
     *
     * 0: uuid string
     * 1: timestamp long
     * 2: node string
     * 3: config string
     */
    val sqlTpl = "insert overwrite configurations values ('%0', %1, '%2', '%3', '%4')"
    insert(values, sqlTpl)

  }
  /**
   * This method leverages Apache Spark to create or
   * update data in Redis "tables"
   */
  private def insert(values:Seq[String], sqlTpl:String):Unit = {
    var insertSql = sqlTpl

    values.zipWithIndex.foreach{ case(value, index) =>
      insertSql = insertSql.replace(s"%$index", value)
    }

    // TODO
    throw new Exception("not implemented yet")

  }

  /** READ OPERATIONS - CONFIGURATION **/

  def readConfigByNode(value:String):String = {

    try {

      val sql = s"select config from configurations where node = '$value'"
      // TODO
      val result:DataFrame = null

      val configs = result.collect.map(row => row.getString(0))
      configs(0)

    } catch {
      case _:Throwable => null
    }

  }

  /** READ OPERATIONS - NODE **/

  /**
   * This method retrieves a specific node instance
   * that is identified by its shared `key`
   */
  def readNodeByKey(value:String):OsqueryNode = {

    try {

      val sql = s"select * from nodes where key = '$value'"
      // TODO
      val result:DataFrame = null

      val nodes = dataframe2Nodes(result)
      nodes(0)

    } catch {
      case _:Throwable => null
    }

  }
  /**
   * This method retrieves a specific node instance
   * that is identified by its shared `secret`
   */
  def readNodeBySecret(value:String):OsqueryNode = {

    try {

      val sql = s"select * from nodes where secret = '$value'"
      // TODO
      val result:DataFrame = null

      val nodes = dataframe2Nodes(result)
      nodes(0)

    } catch {
      case _:Throwable => null
    }

  }
  /**
   * This method determines whether a certain node
   * is still alive
   *
   * interval: pre-defined time interval to describe
   * accepted inactivity
   */
  def readNodeHealth(uuid:String, interval:Long):String = {

    try {

      val sql = s"select checkin from nodes where uuid = '$uuid'"
      // TODO
      val result:DataFrame = null

      val checkins = result.collect.map(row => row.getLong(0))
      val checkin = checkins(0)

      val delta = System.currentTimeMillis - checkin
      if (delta > interval) "danger" else ""

    } catch {
      case _:Throwable => null
    }

  }

  private def dataframe2Nodes(dataset:DataFrame):Array[OsqueryNode] = {

    val rows = dataset.collect()
    val nodes = rows.map(row => {

      val uuid = row.getString(0)
      val timestamp = row.getLong(1)

      val active = row.getBoolean(2)
      val enrolled = row.getBoolean(3)

      val secret = row.getString(4)
      val key = row.getString(5)

      val host = row.getString(6)
      val checkin = row.getLong(7)

      val address = row.getString(8)

      OsqueryNode(
        uuid           = uuid,
        timestamp      = timestamp,
        active         = active,
        enrolled       = enrolled,
        secret         = secret,
        hostIdentifier = host,
        lastCheckIn    = checkin,
        lastAddress    = address,
        nodeKey        = key)

    })

    nodes

  }

  /** READ OPERATIONS - QUERY **/

  /**
   * Retrieve all distributed queries assigned to a particular
   * node in the NEW state. This function will change the state
   * of the distributed query to PENDING, however will not commit
   * the change.
   *
   * It is the responsibility of the caller to commit or rollback
   * on the current database session.
    */
  def getNewQueries(uuid:String):JsonObject = {
    /*
     * STEP #1: Retrieve all distributed query tasks, that refer
     * to the provided node identifier and have status `NEW`, and
     * select all associated queries where 'notBefore < now`
     *
     * QueryTask.node == node AND QueryTask.status = 'NEW' AND Query.notBefore < now
     */
    val now = System.currentTimeMillis
    /*
     * The result is a `Map`that assigns the task `uuid` and
     * the `sql` field of the query
     */
    val result = readNewQueries(uuid, now)

    val queries = new JsonObject()
    result.foreach { case(uuid, sql) =>
      /*
       * The tasks, that refer to the database retrieval result have
       * to be updated; this should be done after the osquery node
       * received the queries, which, however, is not possible
       */
      var task = readTaskById(uuid)
      task = task.copy(status = "PENDING", timestamp = now)

      updateTask(task)
      /*
       * Assign (task uuid, sql) to the output
       */
      queries.addProperty(uuid, sql.asInstanceOf[String])
    }

    queries
  }

  /**
   * This method retrieves queries that have been assigned to
   * a certain node but not deployed to the respective osquery
   * daemon
   */
  private def readNewQueries(uuid:String, timestamp:Long):Map[String,String] = {

    try {

      val sql = s"select tasks.uuid, queries.sql from tasks inner join queries on tasks.query = queries.uuid where tasks.node = '$uuid' and tasks.status = 'NEW' and queries.notbefore < $timestamp"
      // TODO
      val result:DataFrame = null

      val rows = result.collect
      rows.map(row => {

        val task_uuid = row.getString(0)
        val query_sql = row.getString(1)

        (task_uuid, query_sql)

      }).toMap

    } catch {
      case _:Throwable => null
    }

  }

  /** READ OPERATIONS - TASK **/

  def readTaskById(uuid:String):OsqueryQueryTask = {

    try {

      val sql = s"select * from tasks where uuid = '$uuid'"
      // TODO
      val result:DataFrame = null

      val tasks = dataframe2Tasks(result)
      tasks(0)

    } catch {
      case _:Throwable => null
    }

  }

  private def dataframe2Tasks(dataset:DataFrame):Array[OsqueryQueryTask] = {

    val rows = dataset.collect()
    val tasks = rows.map(row => {

      val uuid = row.getString(0)
      val timestamp = row.getLong(1)

      val node = row.getString(2)
      val query = row.getString(3)

      val status = row.getString(4)

      OsqueryQueryTask(
        uuid      = uuid,
        timestamp = timestamp,
        node      = node,
        query     = query,
        status    = status)

    })

    tasks

  }

  def buildTables():Unit = {
    DBUtil.buildTables(ignite, namespace)
  }

}
