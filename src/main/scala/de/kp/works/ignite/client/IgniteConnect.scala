package de.kp.works.ignite.client
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

import de.kp.works.ignite.util.IgniteUtil
import org.apache.ignite._
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.SparkSession

object IgniteConnect {

  private var instance:Option[IgniteConnect] = None

  private var graphNS:Option[String] = None

  def getInstance(session: SparkSession, config: IgniteConfiguration, namespace: String): IgniteConnect = {

    graphNS = Some(namespace)

    if (instance.isEmpty)
      instance = Some(new IgniteConnect(session, config, namespace))

    instance.get

  }

  def getInstance(config: IgniteConfiguration, namespace: String): IgniteConnect = {
    if (instance.isEmpty)
      instance = Some(new IgniteConnect(null, config, namespace))

    instance.get
  }

  def namespace:String = {
    if (graphNS.isDefined) graphNS.get else null
  }
}

class IgniteConnect(
   session: SparkSession,
   config: IgniteConfiguration,
   val graphNS: String) {

  private var ignite:Option[Ignite] = None
  /*
   * The [IgniteContext] supports Apache Spark
   * related features (such as bulk reading of
   * edges and vertices)
   */
  private var igniteContext:Option[IgniteContext] = None
  /*
   * Distinguish an Apache Spark initialization
   * and a plain Ignite initialization
   */
  if (session == null) {
    /*
     * Initialize Ignite with the provided configuration
     */
    ignite = Some(Ignition.getOrStart(config))

  }
  else {
    /*
     * Initialize IgniteContext with the provided configuration
     * and derive Ignite from context
     */
    igniteContext = Some(IgniteContext(session.sparkContext, () => config))
    ignite = Some(igniteContext.get.ignite())

  }

  @throws[Exception]
  def getIgnite: Ignite = {
    if (ignite.isDefined) ignite.get else null
  }

  def getConfig: IgniteConfiguration = {
    config
  }

  def getOrCreateCache(name: String): IgniteCache[String, BinaryObject] = {
    /*
     * .withKeepBinary() must not be used here
     */
    if (ignite.isEmpty) {
      return null
    }
    val cache = IgniteUtil.getOrCreateCache(ignite.get, name, graphNS)
    if (cache == null)
      throw new Exception("Connection to Ignited failed. Could not create cache.");
    /*
     * Rebalancing is called here in case of partitioned
     * Apache Ignite caches; the default configuration,
     * however, is to use replicated caches
     */
    cache.rebalance().get()
    cache
  }

  @throws[Exception]
  def cacheExists(cacheName: String): Boolean = {
    if (ignite.isEmpty) {
      throw new Exception("Connecting Ignite failed.")
    }
    ignite.get.cacheNames.contains(cacheName)
  }

  @throws[Exception]
  def deleteCache(cacheName: String): Unit = {
    if (ignite.isEmpty) {
      throw new Exception("Connecting Ignite failed.")
    }
    val exists: Boolean = ignite.get.cacheNames.contains(cacheName)
    if (exists) {
      ignite.get.cache(cacheName).destroy()
    }
  }

}
