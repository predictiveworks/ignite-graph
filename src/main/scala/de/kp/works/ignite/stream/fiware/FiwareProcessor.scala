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

import com.google.gson.JsonParser
import de.kp.works.conf.WorksConf
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.IgniteProcessor
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable

class FiwareProcessor(
  cache:IgniteCache[String,BinaryObject],
  connect:IgniteConnect) extends IgniteProcessor(cache) {

  private val eventFields = Array(
    "_key",
    FiwareConstants.FIELD_SERVICE,
    FiwareConstants.FIELD_SERVICE_PATH,
    FiwareConstants.FIELD_PAYLOAD).mkString(",")
  /**
   * Apache Ignite SQL query to retrieve the content of
   * the temporary notification cache including the _key
   */
  override protected val eventQuery =
    new SqlFieldsQuery(s"select $eventFields from ${FiwareConstants.FIWARE_CACHE}")
  /**
   * This store is introduced to collect the result from the
   * event query in a distinct manner; the eventStore is used
   * as a buffer before it is flushed and cleared
   */
  private val eventStore = mutable.HashMap.empty[String,FiwareNotification]
  /**
   * The frequency we flush the internal store and write
   * data to the predefined output is currently set to
   * 2 times of the stream buffer flush frequency
   */
  private val conf = WorksConf.getStreamerCfg(WorksConf.FIWARE_CONF)
  override val flushWindow:Int = conf.getInt("flushWindow")

  private val writer = new FiwareWriter(connect)

  /**
   * A helper method to apply the event query to the selected
   * Ignite cache, retrieve the results and write them to the
   * eventStore
   */
  override protected def extractEntries(): Unit = {

    val keys = new java.util.HashSet[String]()
    /*
     * Extract & transform cache entries
     */
    readEvents().foreach(values => {
      val k = values.head.asInstanceOf[String]
      keys.add(k)

      val mutable.Buffer(service, servicePath, payload) =
        values.tail.map(_.asInstanceOf[String])

      eventStore += k -> FiwareNotification(service,servicePath, JsonParser.parseString(payload).getAsJsonObject)
    })
    /*
     * Clear extracted cache entries fast
     */
    cache.clearAll(keys)
  }
  /**
   * A helper method to process the extracted cache entries
   * and transform and write to predefined output
   */
  override protected def processEntries(): Unit = {
    /*
     * Extract events from store, clear store
     * immediately afterwards and send to writer
     */
    val events = eventStore.values.toSeq
    eventStore.clear

    writer.write(events)

  }

}
