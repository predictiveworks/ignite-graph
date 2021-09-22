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

import de.kp.works.conf.WorksConf
import de.kp.works.ignite.Session
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.osquery.{OsqueryEvent, OsqueryWriter}
import org.apache.spark.sql.SaveMode

class FleetWriter(connect:IgniteConnect) extends OsqueryWriter(connect) {

  private val fleetCfg = WorksConf.getCfg(WorksConf.FLEETDM_CONF)

  private val primaryKey = fleetCfg.getString("primaryKey")
  private val tableParameters = fleetCfg.getString("tableParameters")

  override def write(events:Seq[OsqueryEvent]):Unit = {

    try {
      /*
       * STEP #1: Retrieve Spark session and make sure
       * that an Apache Ignite node is started
       */
      val session = Session.getSession
      /*
       * STEP #2: Transform the Osquery events into an
       * Apache Spark compliant format
       */
      val transformed = FleetTransformer.transform(events)
      /*
       * STEP #3: Write logs that refer to a certain Osquery
       * format (RESULT, STATUS) to individual Apache Ignite
       * caches.
       */
      transformed.foreach{case(_, table, schema, rows) =>

        val dataframe = session.createDataFrame(session.sparkContext.parallelize(rows), schema)
        save(table, primaryKey, tableParameters, dataframe, SaveMode.Append)

      }

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

}
