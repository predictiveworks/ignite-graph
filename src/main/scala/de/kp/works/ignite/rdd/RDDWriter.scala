package de.kp.works.ignite.rdd
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
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.{DataFrame, Row}

abstract class RDDWriter(ic:IgniteContext,keepBinary:Boolean = false) {

  protected var cfg:Option[CacheConfiguration[String,BinaryObject]] = None
  protected var table:Option[String] = None

  def save(dataframe:DataFrame, f: (Row, IgniteContext) ⇒ (String, BinaryObject),
           overwrite:Boolean = true,
           skipStore:Boolean = true):Unit = {
    /*
     * IgniteBatchRDD is an application controlled variant
     * of Apache Ignite's [IgniteRDD] that extends this class
     * with a batch receiver for large-scale processing.
     */
    if (cfg.isEmpty)
      throw new Exception("No Ignite cache configuration defined.")

    val cache = new IgniteBatchRDD(ic, table.get, cfg.get, keepBinary)
    cache.savePairs(dataframe.rdd, f, overwrite, skipStore)

  }
}
