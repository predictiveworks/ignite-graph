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

import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.{IgniteCTIContext, IgniteStream, IgniteStreamContext}
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.security.MessageDigest
import java.util.concurrent.TimeUnit.SECONDS
import javax.cache.configuration.FactoryBuilder
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}
import scala.collection.JavaConversions.mapAsJavaMap

/**
 * [CTIIgnite] is responsible for streaming threat intelligence
 * events into a temporary cache and also their final processing
 * as edges & vertices of an information network.
 */
class CTIIgnite(connect:IgniteConnect) {

  if (!CommonConfig.isInit)
    throw new Exception("[CTIIgnite] No configuration initialized. Streaming cannot be started.")

  private val ignite = connect.getIgnite
  private val conf = CommonConfig.getCTIStreamerCfg

  def buildStream:Option[IgniteStreamContext] = {

    try {

      val (cache,streamer) = prepareCTIStreamer
      val numThreads = conf.getInt("numThreads")

      val stream: IgniteStream = new IgniteStream {
        override val processor = new CTIProcessor(cache, connect)
      }

      Some(new IgniteCTIContext(stream,streamer, numThreads))

    } catch {
      case t: Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }
  }

  private def prepareCTIStreamer:(IgniteCache[String,BinaryObject],CTIStreamer[String,BinaryObject]) = {
    /*
     * The auto flush frequency of the stream buffer is
     * internally set to 0.5 sec (500 ms)
     */
    val autoFlushFrequency = conf.getInt("ignite.autoFlushFrequency")
    /*
     * The cache is configured with sliding window holding
     * N seconds of the streaming data; note, that we delete
     * an already equal named cache
     */
    deleteCache()

    val config = createCacheConfig
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
    val ctiStreamer = new CTIStreamer[String,BinaryObject]()

    ctiStreamer.setIgnite(ignite)
    ctiStreamer.setStreamer(streamer)
    /*
     * The OpenCTI extractor is the linking element between the
     * OpenCTI events and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val ctiExtractor = createCTIExtractor
    ctiStreamer.setSingleTupleExtractor(ctiExtractor)

    (cache, ctiStreamer)

  }
  /**
   * Configuration for the threat events cache to store
   * the stream of events. This cache is configured with
   * a sliding window of N seconds, which means that data
   * older than N second will be automatically removed
   * from the cache.
   */
  private def createCacheConfig:CacheConfiguration[String,BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qes = new java.util.ArrayList[QueryEntity]()
    qes.add(buildQueryEntity)
    /*
     * Configure streaming cache.
     */
    val cfg = new CacheConfiguration[String,BinaryObject]()
    cfg.setName(CTIConstants.OPENCTI_CACHE)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    cfg.setStoreKeepBinary(true)
    cfg.setIndexedTypes(classOf[String],classOf[BinaryObject])

    cfg.setQueryEntities(qes)

    cfg.setStatisticsEnabled(true)
    /*
     * The time window specifies the batch window that
     * is used to gather stream events
     */
    val timeWindow = conf.getInt("timeWindow")

    /* Sliding window of 'timeWindow' in seconds */
    val duration = timeWindow / 1000

    cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, duration))))
    cfg

  }
  /**
   * A helper method to build an Apache Ignite QueryEntity
   */
  private def buildQueryEntity:QueryEntity = {

    val queryEntity = new QueryEntity()

    queryEntity.setKeyType("java.lang.String")
    queryEntity.setValueType(CTIConstants.OPENCTI_CACHE)

    val fields = new java.util.LinkedHashMap[String,String]()
    /*
     * The event identifier
     */
    fields.put(CTIConstants.FIELD_ID,"java.lang.String")
    /*
     * The event type
     */
    fields.put(CTIConstants.FIELD_TYPE,"java.lang.String")
    /*
     * The data that is associated with the event
     */
    fields.put(CTIConstants.FIELD_DATA,"java.lang.String")

    queryEntity.setFields(fields)
    queryEntity

  }

  private def createCTIExtractor: StreamSingleTupleExtractor[SseEvent, String, BinaryObject] = {

    new StreamSingleTupleExtractor[SseEvent,String,BinaryObject]() {

      override def extract(event:SseEvent):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val builder = ignite.binary().builder(CTIConstants.OPENCTI_CACHE)
          builder.setField(CTIConstants.FIELD_ID, event.eventId)

          builder.setField(CTIConstants.FIELD_TYPE, event.eventType)
          builder.setField(CTIConstants.FIELD_DATA, event.data)

          val cacheValue = builder.build()
          /*
           * The cache key is built from the content
           * to enable the detection of duplicates.
           *
           * (see CTIProcessor)
           */
          val serialized = Seq(
            event.eventId,
            event.eventType,
            event.data).mkString("#")

          val cacheKey = new String(MessageDigest.getInstance("MD5")
            .digest(serialized.getBytes("UTF-8")))

          entries.put(cacheKey,cacheValue)

        } catch {
          case e:Exception => e.printStackTrace()
        }
        entries.entrySet().iterator().next

      }
    }
  }

  /**
   * This method deletes the temporary notification
   * cache from the Ignite cluster
   */
  private def deleteCache():Unit = {
    try {

      if (ignite.cacheNames().contains(CTIConstants.OPENCTI_CACHE)) {
        val cache = ignite.cache(CTIConstants.OPENCTI_CACHE)
        cache.destroy()
      }

    } catch {
      case t:Throwable => /* do noting */

    }
  }

}
