package de.kp.works.ignite.stream.fiware
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

import de.kp.works.ignite.stream.IgniteProcessor
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.{Ignite, IgniteCache}

import java.util.Properties

class FiwareProcessor(
  cache:IgniteCache[String,BinaryObject],
  ignite:Ignite,
  properties:Properties) extends IgniteProcessor(cache, ignite) {

  private val notificationFields = Array(
    "_key",
    FiwareConstants.FIELD_SERVICE,
    FiwareConstants.FIELD_SERVICE_PATH,
    FiwareConstants.FIELD_PAYLOAD).mkString(",")
  /**
   * Apache Ignite SQL query to retrieve the content of
   * the temporary notification cache including the _key
   */
  private val eventQuery =
    new SqlFieldsQuery(s"select $notificationFields from ${FiwareConstants.FIWARE_CACHE}")
  /**
   * The frequency we flush the internal store and write
   * data to the predefined output is currently set to
   * 2 times of the stream buffer flush frequency
   */
  override val flushWindow:Int = {
    if (properties.containsKey("flushWindow"))
      properties.getProperty("flushWindow").toInt

    else
      DEFAULT_FLUSH_WINDOW.toInt
  }

  /**
   * A helper method to apply the event query to the selected
   * Ignite cache, retrieve the results and write them to the
   * eventStore
   */
  override protected def extractEntries(): Unit = {
    // TODO
  }

  /**
   * A helper method to process the extracted cache entries
   * and transform and write to predefined output
   */
  override protected def processEntries(): Unit = {
    // TODO
  }
}
