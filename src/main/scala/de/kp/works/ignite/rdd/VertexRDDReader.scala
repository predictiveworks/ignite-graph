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

import de.kp.works.ignitegraph.IgniteConstants
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class VertexRDDReader(
   ic:IgniteContext,
   namespace:String,
   cfg:CacheConfiguration[String,BinaryObject]) extends RDDReader(ic, cfg) {

  def vertices():DataFrame = {

    val table = namespace + "_" + IgniteConstants.VERTICES
    val dataframe = load(table, getFields)
    /*
     * The dataframe contains the the cache entries and
     * we want to transform them into a vertex compliant
     * format
     */
    null
  }

  private def getFields:Seq[String] = {

    val fields = ArrayBuffer.empty[String]
    /*
     * The vertex identifier used by TinkerPop to identify
     * an equivalent of a data row
     */
    fields += IgniteConstants.ID_COL_NAME
    /*
     * The vertex identifier type to reconstruct the
     * respective value. IgniteGraph supports [Long]
     * as well as [String] as identifier.
     */
    fields += IgniteConstants.ID_TYPE_COL_NAME
    /*
     * The vertex label used by TinkerPop and IgniteGraph
     */
    fields += IgniteConstants.LABEL_COL_NAME
    /*
     * The timestamp this cache entry has been created.
     */
    fields += IgniteConstants.CREATED_AT_COL_NAME
    /*
     * The timestamp this cache entry has been updated.
     */
    fields += IgniteConstants.UPDATED_AT_COL_NAME
    /*
     * The property section of this cache entry
     */
    fields += IgniteConstants.PROPERTY_KEY_COL_NAME
    fields += IgniteConstants.PROPERTY_TYPE_COL_NAME
    /*
     * The serialized property value
     */
    fields += IgniteConstants.PROPERTY_VALUE_COL_NAME
    /*
     * The [ByteBuffer] representation for the
     * property value is an internal field and
     * not exposed to queries
     */
    fields

  }
}
