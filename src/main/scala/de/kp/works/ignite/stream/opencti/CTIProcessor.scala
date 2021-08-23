package de.kp.works.ignite.stream.opencti
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
import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.client.{IgniteConnect, IgniteTable}
import de.kp.works.ignite.client.mutate.IgnitePut
import de.kp.works.ignite.stream.IgniteProcessor
import de.kp.works.ignitegraph.IgniteConstants
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable

class CTIProcessor(
  cache:IgniteCache[String,BinaryObject],
  connect:IgniteConnect) extends IgniteProcessor(cache) {

  private val eventFields = Array(
    "_key",
    CTIConstants.FIELD_ID,
    CTIConstants.FIELD_TYPE,
    CTIConstants.FIELD_DATA).mkString(",")
  /**
   * Apache Ignite SQL query to retrieve the content of
   * the temporary notification cache including the _key
   */
  private val eventQuery =
    new SqlFieldsQuery(s"select $eventFields from ${CTIConstants.OPENCTI_CACHE}")
  /**
   * This store is introduced to collect the result from the
   * event query in a distinct manner; the eventStore is used
   * as a buffer before it is flushed and cleared
   */
  private val eventStore = mutable.HashMap.empty[String,SseEvent]
  /**
   * The frequency we flush the internal store and write
   * data to the predefined output is currently set to
   * 2 times of the stream buffer flush frequency
   */
  private val conf = CommonConfig.getCTIStreamerCfg
  override val flushWindow:Int = conf.getInt("flushWindow")

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

      val mutable.Buffer(eventId, eventType, data) =
        values.tail.map(_.asInstanceOf[String])

      eventStore += k -> SseEvent(eventId, eventType, data)
    })
    /*
     * Clear extracted cache entries fast
     */
    cache.clearAll(keys)

  }
  /**
   * This method is responsible for retrieving the streaming
   * events, i.e. entries of the Apache Ignite stream cache
   */
  private def readEvents():java.util.List[java.util.List[_]] = {
    /*
     * The default processing retrieves all entries of the
     * Apache Ignite stream cache without preprocessing
     */
    cache.query(eventQuery).getAll
  }

  /**
   * A helper method to process the extracted cache entries
   * and transform and write to predefined output
   */
  override protected def processEntries(): Unit = {
    /*
     * Extract events from store, clear store
     * immediately afterwards and send to
     * transformation stage
     */
    val events = eventStore.values.toSeq
    eventStore.clear
    /*
     * Leverage the CTITransformer to extract
     * vertices and edges from the threat events
     */
    val transformer = CTITransformer
    val (vertices, edges) = transformer.transform(events)
    /*
     * Finally write vertices and edges to the
     * respective output caches
     */
    writeVertices(vertices)
    writeEdges(edges)

  }

  private def writeEdges(edges:Seq[IgnitePut]):Unit = {

    val name = s"${connect.graphNS}_${IgniteConstants.EDGES}"
    val table = new IgniteTable(name, connect)

    edges.foreach(edge => table.put(edge))

  }

  private def writeVertices(vertices:Seq[IgnitePut]):Unit = {

    val name = s"${connect.graphNS}_${IgniteConstants.VERTICES}"
    val table = new IgniteTable(name, connect)

    vertices.foreach(vertex => table.put(vertex))

  }

}
