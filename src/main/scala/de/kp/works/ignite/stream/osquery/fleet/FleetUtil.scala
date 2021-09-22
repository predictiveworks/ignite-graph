package de.kp.works.ignite.stream.osquery.fleet
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.gson.{JsonElement, JsonObject}
import de.kp.works.conf.WorksConf
import de.kp.works.ignite.stream.osquery.OsqueryConstants
import de.kp.works.ignite.stream.osquery.schema.Schema
import de.kp.works.json.JsonUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import scala.collection.JavaConversions._

object FleetUtil {

  private val fleetCfg = WorksConf.getCfg(WorksConf.FLEETDM_CONF)
  private val fleetKey = fleetCfg.getString("primaryKey")

  private val primaryKey = StructField(fleetKey, StringType, nullable = false)
  private val columnMap = Schema.getTypes

  def fromResult(logs:Seq[JsonElement]):Seq[(String, StructType, Seq[Row])] = {

    logs
      /*
       * A certain log result refers to a predefined query (configuration or pack).
       * Each query addresses a certain Osquery table (name) and a set of configured
       * columns. This implementation expects that a specific table always occurs
       * with the same (predefined) columns (see schema).
       *
       * A sequence of logs, streamed from the filesystem, can contain query or table
       * results that refer to different tables. Therefore, the result is grouped by
       * the table name.
       *
       * Finally, the table name is also used as the name of the respective Apache
       * Ignite cache.
       */
      .map(log => fromResult(log.getAsJsonObject))
      /*
       * Group the transformed results with respect to the query or table name
       */
      .groupBy { case (name, _, _) => name }
      .map { case (name, values) =>
        val schema = values.head._2
        val rows = values.flatMap(_._3)

        (name, schema, rows)
      }
      .toSeq

  }
  /**
   * This method accepts that each log object provided
   * is different with respect to its data format
   */
  def fromResult(oldObject:JsonObject):(String, StructType, Seq[Row]) = {

    val commonObject = new JsonObject
    /*
     * Extract `name` (of the query) and normalize `calendarTime`:
     *
     * "calendarTime": "Tue Sep 30 17:37:30 2014"
     *
     * - Weekday as locale’s abbreviated name
     *
     * - Month as locale’s abbreviated name
     *
     * - Day of the month as a zero-padded decimal number
     *
     * - H:M:S
     *
     * - Year with century as a decimal number
     *
     * UTC
     */
    val name = oldObject.get(OsqueryConstants.NAME).getAsString
    commonObject.addProperty(OsqueryConstants.NAME, name)

    val calendarTime = oldObject.get(OsqueryConstants.CALENDAR_TIME).getAsString
    val datetime = transformCalTime(calendarTime)

    val timestamp = datetime.getTime
    commonObject.addProperty(OsqueryConstants.TIMESTAMP, timestamp)

    val hostname = getHostname(oldObject)
    commonObject.addProperty(OsqueryConstants.HOSTNAME, hostname)

    /*
     * In this case, `event format`, columns is a single
     * object that must be added to the overall output
     *
     * {
     *  "action": "added",
     *  "columns": {
     *    "name": "osqueryd",
     *    "path": "/opt/osquery/bin/osqueryd",
     *    "pid": "97830"
     *  },
     *  "name": "processes",
     *  "hostname": "hostname.local",
     *  "calendarTime": "Tue Sep 30 17:37:30 2014",
     *  "unixTime": "1412123850",
     *  "epoch": "314159265",
     *  "counter": "1",
     *  "numerics": false
     * }
     */
    val (schema, rows) = if (oldObject.get(OsqueryConstants.COLUMNS) != null) {

      val action = oldObject.get(OsqueryConstants.ACTION).getAsString

      val rowObject = commonObject
      rowObject.addProperty(OsqueryConstants.ACTION, action)
      /*
       * Extract log event specific format and thereby
       * assume that the columns provided are the result
       * of an offline configuration process.
       *
       * NOTE: the mechanism below does not work for adhoc
       * (distributed) queries.
       */
      val columns = oldObject.get(OsqueryConstants.COLUMNS).getAsJsonObject
      /*
       * Extract the column names in ascending order
       */
      val colnames = columns.keySet.toSeq.sorted
      colnames.foreach(colName => rowObject.add(colName, columns.get(colName)))

      val schema = result(colnames)
      val row = JsonUtil.json2Row(rowObject, schema)

      (schema, Seq(row))

    }
    /*
     * If a query identifies multiple state changes, the batched format
     * will include all results in a single log line.
     *
     * If you're programmatically parsing lines and loading them into a
     * backend datastore, this is probably the best solution.
     *  {
     *    "diffResults": {
     *       "added": [
     *         {
     *           "name": "osqueryd",
     *           "path": "/opt/osquery/bin/osqueryd",
     *           "pid": "97830"
     *         }
     *       ],
     *       "removed": [
     *         {
     *           "name": "osqueryd",
     *           "path": "/opt/osquery/bin/osqueryd",
     *           "pid": "97650"
     *         }
     *       ]
     *    },
     *    "name": "processes",
     *    "hostname": "hostname.local",
     *    "calendarTime": "Tue Sep 30 17:37:30 2014",
     *    "unixTime": "1412123850",
     *    "epoch": "314159265",
     *    "counter": "1",
     *    "numerics": false
     *  }
     */
    else if (oldObject.get(OsqueryConstants.DIFF_RESULTS) != null) {

      val diffResults = oldObject.get(OsqueryConstants.DIFF_RESULTS).getAsJsonObject
      /*
       * The subsequent transformation assumes that the columns
       * specified in the query result, independent of the data
       * action, is always the same.
       */
      val actions = diffResults.keySet().toSeq.sorted
      var schema:StructType = null

      val rows = actions.flatMap(action => {

        val data = diffResults.get(action).getAsJsonArray
        data.map(columns => {

          val rowObject = commonObject
          rowObject.addProperty(OsqueryConstants.ACTION, action)

          val colnames = columns.getAsJsonObject.keySet.toSeq.sorted
          colnames.foreach(colName => rowObject.add(colName, columns.getAsJsonObject.get(colName)))

          if (schema == null) schema = result(colnames)
          JsonUtil.json2Row(rowObject, schema)

        })

      })

      (schema, rows)

    }
    /*
     * Snapshot queries attempt to mimic the differential event format,
     * instead of emitting "columns", the snapshot data is stored using
     * "snapshot".
     *
     *  {
     *    "action": "snapshot",
     *    "snapshot": [
     *      {
     *        "parent": "0",
     *         "path": "/sbin/launchd",
     *        "pid": "1"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/sbin/syslogd",
     *        "pid": "51"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/libexec/UserEventAgent",
     *        "pid": "52"
     *      },
     *      {
     *        "parent": "1",
     *        "path": "/usr/libexec/kextd",
     *        "pid": "54"
     *      }
     *    ],
     *    "name": "process_snapshot",
     *    "hostIdentifier": "hostname.local",
     *    "calendarTime": "Mon May  2 22:27:32 2016 UTC",
     *    "unixTime": "1462228052",
     *    "epoch": "314159265",
     *    "counter": "1",
     *    "numerics": false
     *  }
     */
    else if (oldObject.get(OsqueryConstants.SNAPSHOT) != null) {

      val data = oldObject.get(OsqueryConstants.SNAPSHOT).getAsJsonArray
      /*
       * The subsequent transformation assumes that the columns
       * specified in the query result, independent of the data
       * action, is always the same.
       */
      var schema:StructType = null
      val rows = data.map(columns => {

        val rowObject = commonObject
        rowObject.addProperty(OsqueryConstants.ACTION, OsqueryConstants.SNAPSHOT)

        val colnames = columns.getAsJsonObject.keySet.toSeq.sorted
        colnames.foreach(colName => rowObject.add(colName, columns.getAsJsonObject.get(colName)))

        if (schema == null) schema = result(colnames)
        JsonUtil.json2Row(rowObject, schema)

      }).toSeq

      (schema, rows)

    }
    else {
      throw new Exception("Query result encountered that cannot be transformed.")
    }

    (name, schema, rows)

  }

  def result(columns:Seq[String]):StructType = {
    /*
     * COMMON FIELDS
     */
    var fields = Array(
      /* The `name` (of the query)
       */
      StructField(OsqueryConstants.NAME, StringType, nullable = false),
      /* The timestamp of the event
       */
      StructField(OsqueryConstants.TIMESTAMP, LongType, nullable = false),
      /* The hostname of the event
       */
      StructField(OsqueryConstants.HOSTNAME, StringType, nullable = false),
      /* The action of the event
       */
      StructField(OsqueryConstants.ACTION, StringType, nullable = false)
    )
    /*
     * REQUEST SPECIFIC FIELDS
     *
     * The fields are sorted in ascending order to match
     * the provided log message.
     *
     * As the columns exist, we specify their fields as
     * nullable = false
     */
    fields = fields ++ columns
      .map(column => StructField(column, columnMap(column), nullable = false))

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
 /**
  * Osquery creates status logs of its own execution, for log levels
  * INFO, WARNING and ERROR. Note, this implementation expects a single
  * log file that contains status messages of all levels.
  */
  def fromStatus(logs:Seq[JsonElement]):Seq[(String, StructType, Seq[Row])] = {
    logs
      .map(log => fromStatus(log.getAsJsonObject))
      /*
       * Group the transformed results with respect to the query or table name
       */
      .groupBy { case (name, _, _) => name }
      .map { case (name, values) =>
        val schema = values.head._2
        val rows = values.flatMap(_._3)

        (name, schema, rows)
      }
      .toSeq

  }

  def fromStatus(oldObject:JsonObject):(String, StructType, Seq[Row]) = {

    val rowObject = new JsonObject
    rowObject.addProperty(OsqueryConstants.MESSAGE, oldObject.toString)

    val schema = status()
    val row = JsonUtil.json2Row(rowObject, schema)

    ("status_log", schema, Seq(row))

  }

  def status():StructType = {
    /*
     * COMMON FIELDS
     */
    var fields = Array(
      /* The `name` (of the query)
       */
      StructField(OsqueryConstants.MESSAGE, StringType, nullable = false)
    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /** HELPER METHODS **/

  private def getHostname(oldObject:JsonObject):String = {
    /*
     * The host name is represented by different
     * keys within the different result logs
     */
    val keys = Array(
      OsqueryConstants.HOST,
      OsqueryConstants.HOST_IDENTIFIER,
      OsqueryConstants.HOSTNAME)

    try {

      val key = oldObject.keySet()
        .filter(k => keys.contains(k))
        .head

      oldObject.get(key)
        .getAsString

    } catch {
      case _:Throwable => ""
    }
  }

  private def transformCalTime(s: String): java.util.Date = {

    try {

      /* Osquery use UTC to describe datetime */
      val pattern = if (s.endsWith("UTC")) {
        "EEE MMM dd hh:mm:ss yyyy z"
      }
      else
        "EEE MMM dd hh:mm:ss yyyy"

      val format = new SimpleDateFormat(pattern, java.util.Locale.US)
      format.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

      format.parse(s)

    } catch {
      case _: Throwable => null
    }

  }

}
