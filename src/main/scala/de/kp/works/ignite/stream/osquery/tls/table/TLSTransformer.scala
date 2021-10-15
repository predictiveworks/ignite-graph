package de.kp.works.ignite.stream.osquery.tls.table
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

import de.kp.works.ignite.stream.osquery.tls.TLSEvent

object TLSTransformer {
  /**
   * This method receives a node and its query result `data`
   * and converts the incoming log data into a series of fields,
   * normalizing and/or aggregating both batch and event format
   * into batch format, which is used throughout the rest of this
   * service.
   */
  def transform(events: Seq[TLSEvent]): Unit = ???

}
