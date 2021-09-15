package de.kp.works.ignite.stream.osquery.db
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

import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.{CacheMode, QueryEntity}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}

import java.util

object DBUtil {

  /**
   * The actual Osquery database consists tables to manage
   * `nodes`, `queries`, `tasks` and `configurations`.
   *
   * `tasks` specifies the relation between a certain node
   * and query.
   */
  def buildTables(ignite:Ignite, namespace:String):Unit = {

    buildNodes(ignite, namespace)
    buildQueries(ignite, namespace)

    buildTasks(ignite, namespace)
    buildConfigurations(ignite, namespace)

  }

  def buildNodes(ignite:Ignite, namespace:String):Unit = {

    val table = "nodes"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",      "java.lang.String")
    columns.put("timestamp", "java.lang.Long")
    columns.put("active",    "java.lang.Boolean")
    columns.put("enrolled",  "java.lang.Boolean")
    columns.put("secret",    "java.lang.String")
    columns.put("key",       "java.lang.String")
    columns.put("host",      "java.lang.String")
    columns.put("checkin",   "java.lang.Long")
    columns.put("address",   "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildQueries(ignite:Ignite, namespace:String):Unit = {

    val table = "queries"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("description", "java.lang.String")
    columns.put("sql",         "java.lang.String")
    columns.put("notbefore",   "java.lang.Long")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildTasks(ignite:Ignite, namespace:String):Unit = {

    val table = "tasks"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("node",        "java.lang.String")
    columns.put("query",       "java.lang.String")
    columns.put("status",      "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def buildConfigurations(ignite:Ignite, namespace:String):Unit = {

    val table = "nodes"
    val columns = new util.LinkedHashMap[String, String]

    columns.put("uuid",        "java.lang.String")
    columns.put("timestamp",   "java.lang.Long")
    columns.put("node",        "java.lang.String")
    columns.put("config",      "java.lang.String")

    createCacheIfNotExists(ignite, table, namespace, columns)

  }

  def createCacheIfNotExists(
    ignite:Ignite,
    table:String,
    cfg:CacheConfiguration[String, BinaryObject]): Unit = {

    val exists: Boolean = ignite.cacheNames.contains(table)
    if (!exists) ignite.createCache(cfg)

  }

  def createCacheIfNotExists(
    ignite:Ignite,
    table: String,
    namespace:String,
    columns:util.LinkedHashMap[String, String]): Unit = {

    val exists: Boolean = ignite.cacheNames.contains(table)
    if (!exists) {
      createCache(ignite, table, namespace, columns)
    }
  }

  def createCache(
    ignite:Ignite,
    table: String,
    namespace:String,
    columns:util.LinkedHashMap[String, String]): IgniteCache[String, BinaryObject] = {
      createCache(ignite, table, namespace, columns, CacheMode.REPLICATED)
  }

  def createCache(
     ignite:Ignite,
     table: String,
     namespace:String,
     columns:util.LinkedHashMap[String, String],
     cacheMode: CacheMode): IgniteCache[String, BinaryObject] = {

    val cacheName = namespace + "_" + table
    val cfg = createCacheCfg(cacheName, columns, cacheMode)

    ignite.createCache(cfg)

  }

  /**
   * This method creates an Ignite cache configuration
   * for a database tables that does not exist
   */
  def createCacheCfg(
    cacheName: String,
    columns:util.LinkedHashMap[String, String],
    cacheMode: CacheMode): CacheConfiguration[String, BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qe = buildQueryEntity(cacheName, columns)
    val qes = new util.ArrayList[QueryEntity]
    qes.add(qe)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    val cfg = new CacheConfiguration[String, BinaryObject]
    cfg.setName(cacheName)

    cfg.setStoreKeepBinary(false)
    cfg.setIndexedTypes(classOf[String], classOf[BinaryObject])

    cfg.setCacheMode(cacheMode)
    cfg.setQueryEntities(qes)

    cfg
  }

  private def buildQueryEntity(table: String, columns:util.LinkedHashMap[String, String]): QueryEntity = {

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
    qe.setFields(columns)
    qe

  }

}
