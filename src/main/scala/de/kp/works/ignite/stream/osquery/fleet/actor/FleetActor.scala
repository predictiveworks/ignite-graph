package de.kp.works.ignite.stream.osquery.fleet.actor
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

import com.google.gson.JsonParser
import de.kp.works.conf.WorksConf
import de.kp.works.ignite.stream.FileActor
import de.kp.works.ignite.stream.osquery.{OsqueryEvent, OsqueryEventHandler}

import java.nio.file.Path

class FleetActor(path:Path, eventHandler: OsqueryEventHandler) extends FileActor(WorksConf.FLEETDM_CONF, path) {

  override protected def send(line:String):Unit = {
    try {
      /*
       * Check whether the provided line is
       * a JSON line
       */
      val json = JsonParser.parseString(line)

      val event = OsqueryEvent(eventType = path.toFile.getName, eventData = json.toString)
      eventHandler.eventArrived(event)

    } catch {
      case _:Throwable => /* Do nothing */
    }
  }

}
