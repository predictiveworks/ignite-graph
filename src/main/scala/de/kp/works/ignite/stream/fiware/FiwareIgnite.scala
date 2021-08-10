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
import de.kp.works.ignite.stream.{IgniteFiwareContext, IgniteStream, IgniteStreamContext}

import javax.cache.configuration.FactoryBuilder
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}
import java.util.concurrent.TimeUnit.SECONDS
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.stream.StreamSingleTupleExtractor

import java.util.Properties
import scala.collection.JavaConversions.mapAsJavaMap
/**
 * [FiwareIgnite] is responsible for streaming Orion Broker
 * notifications into a temporary cache and also their final
 * processing as edges & vertices of an information network.
 */
class FiwareIgnite(ignite:Ignite) {
  /**
   * Properties:
   *
   * - timeWindow
   * - ignite.autoFlushFrequency
   * - ignite.numThreads
   */
  def buildStream(props:Properties):Option[IgniteStreamContext] = {

    try {

      val (cache,streamer) = prepareFiwareStreamer(props)
      val numThreads = {
        if (props.containsKey("ignite.numThreads"))
          props.getProperty("ignite.numThreads").toInt

        else
          1
      }
      val stream: IgniteStream = new IgniteStream {
        override val processor = new FiwareProcessor(cache,ignite, props)
      }

      Some(new IgniteFiwareContext(stream,streamer, numThreads))

    } catch {
      case t:Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }

  }

  private def prepareFiwareStreamer(props:Properties):(IgniteCache[String,BinaryObject],FiwareStreamer[String,BinaryObject]) = {
    /*
     * The time window specifies the batch window that
     * is used to gather stream events
     */
    val timeWindow = props.getProperty("timeWindow").toInt
    /*
     * The auto flush frequency of the stream buffer is
     * internally set to 0.5 sec (500 ms)
     */
    val autoFlushFrequency = props.getProperty("ignite.autoFlushFrequency").toInt
    /*
     * The cache is configured with sliding window holding
     * N seconds of the streaming data; note, that we delete
     * an already equal named cache
     */
    deleteCache()

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
    val fiwareStreamer = new FiwareStreamer[String,BinaryObject]()

    fiwareStreamer.setIgnite(ignite)
    fiwareStreamer.setStreamer(streamer)
    /*
     * The Fiware extractor is the linking element between the
     * Fiware notification and its specification as Apache Ignite
     * cache entry.
     *
     * We currently leverage a single tuple extractor as we do
     * not have experience whether we should introduce multiple
     * tuple extraction. Additional performance requirements can
     * lead to a channel in the selected extractor
     */
    val fiwareExtractor = createFiwareExtractor
    fiwareStreamer.setSingleTupleExtractor(fiwareExtractor)

    (cache, fiwareStreamer)
  }
  /**
   * Configuration for the notification cache to store
   * the stream of notifications. This cache is configured
   * with a sliding window of N seconds, which means that
   * data older than N second will be automatically removed
   * from the cache.
   */
  private def createCacheConfig(timeWindow:Int):CacheConfiguration[String,BinaryObject] ={
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qes = new java.util.ArrayList[QueryEntity]()
    qes.add(buildQueryEntity)
    /**
     * Configure streaming cache.
     */
    val cfg = new CacheConfiguration[String,BinaryObject]()
    cfg.setName(FiwareConstants.FIWARE_CACHE)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    cfg.setStoreKeepBinary(true)
    cfg.setIndexedTypes(classOf[String],classOf[BinaryObject])

    cfg.setQueryEntities(qes)

    cfg.setStatisticsEnabled(true)

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
    queryEntity.setValueType(FiwareConstants.FIWARE_CACHE)

    val fields = new java.util.LinkedHashMap[String,String]()
    /*
     * The service that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_SERVICE,"java.lang.String")
    /*
     * The service path that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_SERVICE_PATH,"java.lang.String")
    /*
     * The payload that is associated
     * with the notification
     */
    fields.put(FiwareConstants.FIELD_PAYLOAD,"java.lang.String")

    queryEntity.setFields(fields)
    queryEntity

  }

  private def createFiwareExtractor: StreamSingleTupleExtractor[FiwareNotification, String, BinaryObject] = {

    new StreamSingleTupleExtractor[FiwareNotification,String,BinaryObject]() {

      override def extract(notification:FiwareNotification):java.util.Map.Entry[String,BinaryObject] = {

        val entries = scala.collection.mutable.HashMap.empty[String,BinaryObject]
        try {

          val builder = ignite.binary().builder(FiwareConstants.FIWARE_CACHE)
          builder.setField(FiwareConstants.FIELD_SERVICE, notification.service)

          builder.setField(FiwareConstants.FIELD_SERVICE_PATH, notification.servicePath)
          builder.setField(FiwareConstants.FIELD_PAYLOAD, notification.payload.toString)

          val cacheKey = java.util.UUID.randomUUID.toString
          val cacheValue = builder.build()

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

      if (ignite.cacheNames().contains(FiwareConstants.FIWARE_CACHE)) {
        val cache = ignite.cache(FiwareConstants.FIWARE_CACHE)
        cache.destroy()
      }

    } catch {
      case t:Throwable => /* do noting */

    }
  }

}
