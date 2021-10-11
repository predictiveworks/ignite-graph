package de.kp.works.ignite.stream.osquery
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
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.osquery.fleet.FleetTableWriter
import de.kp.works.ignite.stream.osquery.tls.TLSWriter

object OsqueryWriterFactory {
  /*
   * The current implementation of the Writer factory supports
   * Fleet and TLS server based Osquery log events.
   */
  def get(name:String, connect:IgniteConnect):OsqueryWriter = {

    name match {
      case WorksConf.FLEETDM_CONF =>
        new FleetTableWriter(connect)

      case WorksConf.OSQUERY_CONF =>
        new TLSWriter(connect)

      case _ => throw new Exception(s"Osquery writer for `$name` is not supported.")
    }
  }
}
