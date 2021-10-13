package de.kp.works.ignite.stream.osquery.fleet
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.file.FileEvent
import de.kp.works.ignite.stream.osquery.fleet.table.FleetTableWriter
import de.kp.works.ignite.stream.zeek.table.ZeekTableWriter

class FleetWriter(connect:IgniteConnect) {

  private val fleetCfg = WorksConf.getCfg(WorksConf.FLEETDM_CONF)
  private val writeMode = fleetCfg.getString("writeMode")

  def write(events:Seq[FileEvent]):Unit = {

    writeMode match {
      case "graph" =>
        throw new Exception(s"Not open source yet.")
      case "table" =>
        val writer = new FleetTableWriter(connect)
        writer.write(events)
      case _ =>
        throw new Exception(s"The configured writeMode `$writeMode` is not supported.")
    }

  }

}
