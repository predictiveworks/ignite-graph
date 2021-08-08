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

import de.kp.works.ignitegraph.IgniteConstants
import org.apache.ignite._
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.SparkSession

import java.util

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
   graphNS: String) {

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
    val exists: Boolean = ignite.get.cacheNames.contains(name)
    if (exists) {
      ignite.get.cache(name)
    }
    else {
      createCache(name)
    }
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

  @throws[Exception]
  private def createCache(cacheName: String): IgniteCache[String, BinaryObject] = {
    createCache(cacheName, CacheMode.REPLICATED)
  }

  @throws[Exception]
  private def createCache(cacheName: String, cacheMode: CacheMode): IgniteCache[String, BinaryObject] = {
    val cfg: CacheConfiguration[String, BinaryObject] = createCacheCfg(cacheName, cacheMode)
    ignite.get.createCache(cfg)
  }

  @throws[Exception]
  private def createCacheCfg(table: String, cacheMode: CacheMode) = {
    /*
      * Defining query entities is the Apache Ignite
      * mechanism to dynamically define a queryable
      * 'class'
      */
val qe = buildQueryEntity(table)
    val qes = new util.ArrayList[QueryEntity]
    qes.add(qe)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    val cfg = new CacheConfiguration[String, BinaryObject]
    cfg.setName(table)

    cfg.setStoreKeepBinary(false)
    cfg.setIndexedTypes(classOf[String], classOf[BinaryObject])

    cfg.setCacheMode(cacheMode)
    cfg.setQueryEntities(qes)

    cfg
  }

  /**
   * This method supports the creation of different
   * query entities (or cache schemas)
   */
  @throws[Exception]
  private def buildQueryEntity(table: String) = {

    val qe = new QueryEntity
    /*
     * The key type of the Apache Ignite cache is set to
     * [String], i.e. an independent identity management is
     * used here
     */
    qe.setKeyType("java.lang.String")
    /*
     * The 'table' is used as table name in select statement
     * as well as the name of 'ValueType'
     */
    qe.setValueType(table)
    /*
     * Define fields for the Apache Ignite cache that is
     * used as one of the data backends.
     */
    if (table == graphNS + "_" + IgniteConstants.EDGES)
      qe.setFields(buildEdgeFields)

    else if (table == graphNS + "_" + IgniteConstants.VERTICES)
      qe.setFields(buildVertexFields())

    else
      throw new Exception("Table '" + table + "' is not supported.")

    qe
  }

  private def buildEdgeFields = {

    val fields = new util.LinkedHashMap[String, String]
    /*
     * The edge identifier used by TinkerPop to
     * identify an equivalent of a data row
     */
    fields.put(IgniteConstants.ID_COL_NAME, "java.lang.String")
    /*
     * The edge identifier type to reconstruct the
     * respective value. IgniteGraph supports [Long]
     * as well as [String] as identifier.
     */
    fields.put(IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String")
    /*
     * The edge label used by TinkerPop and IgniteGraph
     */
    fields.put(IgniteConstants.LABEL_COL_NAME, "java.lang.String")
    /*
     * The `TO` vertex description
     */
    fields.put(IgniteConstants.TO_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.TO_TYPE_COL_NAME, "java.lang.String")
    /*
     * The `FROM` vertex description
     */
    fields.put(IgniteConstants.FROM_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.FROM_TYPE_COL_NAME, "java.lang.String")
    /*
             * The timestamp this cache entry has been created.
             */
    fields.put(IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The timestamp this cache entry has been updated.
     */
    fields.put(IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long")
    /*
     * The property section of this cache entry
     */
    fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String")
    fields.put(IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String")
    /*
     * The serialized property value
     */
    fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String")
    fields
  }

  private def buildVertexFields(): util.LinkedHashMap[String, String] = {

     val fields: util.LinkedHashMap[String, String] = new util.LinkedHashMap[String, String]()
     /*
      * The vertex identifier used by TinkerPop to identify
      * an equivalent of a data row
      */
     fields.put (IgniteConstants.ID_COL_NAME, "java.lang.String")
     /*
      * The vertex identifier type to reconstruct the
      * respective value. IgniteGraph supports [Long]
      * as well as [String] as identifier.
      */
     fields.put (IgniteConstants.ID_TYPE_COL_NAME, "java.lang.String")
     /*
      * The vertex label used by TinkerPop and IgniteGraph
      */
     fields.put (IgniteConstants.LABEL_COL_NAME, "java.lang.String")
     /*
      * The timestamp this cache entry has been created.
      */
     fields.put (IgniteConstants.CREATED_AT_COL_NAME, "java.lang.Long")
     /*
      * The timestamp this cache entry has been updated.
      */
     fields.put (IgniteConstants.UPDATED_AT_COL_NAME, "java.lang.Long")
     /*
      * The property section of this cache entry
      */
     fields.put (IgniteConstants.PROPERTY_KEY_COL_NAME, "java.lang.String")
     fields.put (IgniteConstants.PROPERTY_TYPE_COL_NAME, "java.lang.String")
     /*
      * The serialized property value
      */
     fields.put (IgniteConstants.PROPERTY_VALUE_COL_NAME, "java.lang.String")
     fields
   }

 }
