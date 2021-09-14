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

import de.kp.works.conf.WorksConf
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.{BaseEngine, IgniteStream, IgniteStreamContext}
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.util
import scala.collection.JavaConversions._

class ZeekEngine(connect:IgniteConnect) extends BaseEngine(connect) {
  /*
   * The name of the temporary cache to write Zeek log events to
   */
  override protected var cacheName: String = ZeekConstants.ZEEK_CACHE

  if (!WorksConf.isInit)
    throw new Exception("[ZeekEngine] No configuration initialized. Streaming cannot be started.")

  private val conf = WorksConf.getStreamerCfg(WorksConf.ZEEK_CONF)
  /**
   * This is the main method to build the Zeek
   * streaming service (see ZeekStream object).
   *
   * The respective [IgniteZeekContext] combines
   * the plain Ignite streamer with the cache and its
   * specific processor.
   *
   * The context also comprises the connector to the
   * Zeek log event stream.
   */
  override def buildStream: Option[IgniteStreamContext] = {

    try {

      val (cache,streamer) = prepareStreamer
      val numThreads = conf.getInt("numThreads")

      val stream: IgniteStream = new IgniteStream {
        override val processor = new ZeekProcessor(cache, connect)
      }

      Some(new ZeekStreamContext(stream,streamer, numThreads))

    } catch {
      case t:Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }

  }

  private def prepareStreamer:(IgniteCache[String,BinaryObject],ZeekStreamer[String,BinaryObject]) = {
    /*
     * The auto flush frequency of the stream buffer is
     * internally set to 0.5 sec (500 ms)
     */
    val autoFlushFrequency = conf.getInt("autoFlushFrequency")
    /*
     * The cache is configured with sliding window holding
     * N seconds of the streaming data; note, that we delete
     * an already equal named cache
     */
    deleteCache()
    /*
     * The time window specifies the batch window that
     * is used to gather stream events
     */
    val timeWindow = conf.getInt("timeWindow")

    val config = createCacheConfig(timeWindow)
    val cache = ignite.getOrCreateCache(config)

    val streamer = ignite.dataStreamer[String,BinaryObject](cache.getName)
    /*
     * allowOverwrite(boolean) - Sets flag enabling overwriting
     * existing values in cache. Data streamer will perform better
     * if this flag is disabled, which is the default setting.
     */
    streamer.allowOverwrite(false)
    /*
     * IgniteDataStreamer buffers the data and most likely it just
     * waits for buffers to fill up. We set the time interval after
     * which buffers will be flushed even if they are not full
     */
    streamer.autoFlushFrequency(autoFlushFrequency)
    val zeekStreamer = new ZeekStreamer[String,BinaryObject]()

    zeekStreamer.setIgnite(ignite)
    zeekStreamer.setStreamer(streamer)
    /*
     * The Zeek extractor is the linking element between the
     * Zeek log events and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val zeekExtractor = createExtractor
    zeekStreamer.setSingleTupleExtractor(zeekExtractor)

    (cache, zeekStreamer)
  }

  private def createExtractor: StreamSingleTupleExtractor[ZeekEvent, String, BinaryObject] = {

    new StreamSingleTupleExtractor[ZeekEvent,String,BinaryObject]() {

      override def extract(event:ZeekEvent):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val (cacheKey, cacheValue) = buildEntry(event)
          entries.put(cacheKey,cacheValue)

        } catch {
          case e:Exception => e.printStackTrace()
        }
        entries.entrySet().iterator().next

      }
    }

  }

  private def buildEntry(event:ZeekEvent):(String, BinaryObject) = ???

  override protected def buildFields(): util.LinkedHashMap[String, String] = ???

}
