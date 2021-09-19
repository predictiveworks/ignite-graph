package de.kp.works.ignite.stream.zeek
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

import de.kp.works.ignite.Session
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.TableWriter

class ZeekWriter(connect:IgniteConnect) extends TableWriter(connect) {

  def write(events:Seq[ZeekEvent]):Unit = {

    try {
      /*
       * STEP #1: Retrieve Spark session and make sure
       * that an Apache Ignite node is started
       */
      val session = Session.getSession
      /*
       * STEP #1: Transform the Zeek events into an
       * Apache Spark compliant format
       */
      val transformed = ZeekTransformer.transform(events)
      /*
       * STEP #2: Write logs that refer to a certain Zeek
       * format to individual Apache Ignite caches
       */
      transformed.foreach{case(format, schema, rows) => {
        // TODO

      }}

    } catch {
      case _:Throwable => /* Do nothing */
    }

  }

}
