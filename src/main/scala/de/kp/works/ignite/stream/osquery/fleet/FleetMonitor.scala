package de.kp.works.ignite.stream.osquery.fleet
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
import akka.actor.{ActorRef, Props}
import de.kp.works.conf.WorksConf
import de.kp.works.ignite.stream.FileMonitor
import de.kp.works.ignite.stream.osquery.OsqueryEventHandler
import de.kp.works.ignite.stream.osquery.fleet.actor.FleetActor

import java.nio.file.Path

class FleetMonitor(folder: String, eventHandler: OsqueryEventHandler) extends FileMonitor(WorksConf.FLEETDM_CONF, folder) {
  /**
   * A helper method to build a file listener actor
   */
  override protected def buildFileActor(path:Path):ActorRef = {
    val actorName = "File-Actor-" + java.util.UUID.randomUUID.toString
    system.actorOf(Props(new FleetActor(path, eventHandler)), actorName)
  }
}
