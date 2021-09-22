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

import com.google.gson.JsonParser
import de.kp.works.ignite.stream.osquery.OsqueryEvent
import de.kp.works.ignite.stream.osquery.fleet.FleetFormats.{FleetFormat, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object FleetTransformer {

  def transform(events:Seq[OsqueryEvent]):Seq[(FleetFormat, String, StructType, Seq[Row])] = {

    try {
      /*
       * STEP #1: Collect Fleet log events with respect
       * to their eventType (which refers to the name
       * of the log file)
       */
      val data = events
        /*
         * Convert `eventType` (file name) into Fleet format
         * and deserialize event data. Restrict to those
         * formats that are support by the current version.
         */
        .map(event =>
          (FleetFormatUtil.fromFile(event.eventType), JsonParser.parseString(event.eventData))
        )
        .filter{case(format, _) => format != null}
        /*
         * Group logs by format and prepare format specific
         * log processing
         */
        .groupBy{case(format, _) => format}
        .map{case(format, logs) => (format, logs.map(_._2))}
        .toSeq
      /*
       * STEP #2: Persist logs for each format individually
       */
      data.flatMap{case(format, logs) =>

        format match {
          case RESULT =>
            val transformed = FleetUtil.fromResult(logs)
            transformed.map{case(name, schema, rows) => (format, name, schema, rows)}

          case STATUS =>
            val transformed = FleetUtil.fromStatus(logs)
            transformed.map{case(name, schema, rows) => (format, name, schema, rows)}

          case _ => throw new Exception(s"[FleetTransformer] Unknown format `$format.toString` detected.")
        }
      }

    } catch {
      case _:Throwable => Seq.empty[(FleetFormat, String, StructType, Seq[Row])]
    }

  }

}
