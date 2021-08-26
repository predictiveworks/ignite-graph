package de.kp.works.ignite.stream.opencti.transformer
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

import de.kp.works.ignite.client.mutate.IgnitePut
import de.kp.works.ignitegraph.{ElementType, IgniteConstants}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.collection.mutable

trait BaseTransformer {

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[BaseTransformer])

  val EXTERNAL_REFERENCE:String = "external-reference"
  val KILL_CHAIN_PHASE:String   = "kill-chain-phase"
  val OBJECT_LABEL:String       = "object-label"
  /**
   * INTERNAL EDGE LABELS
   */
  val HAS_CREATED_BY:String         = "has-created-by"
  val HAS_EXTERNAL_REFERENCE:String = "has-external-reference"
  val HAS_KILL_CHAIN_PHASE:String   = "has-kill-chain-phase"
  val HAS_OBJECT_LABEL:String       = "has-object-label"
  val HAS_OBJECT_MARKING:String     = "has-object-marking"
  val HAS_OBJECT_REFERENCE:String   = "has-object-reference"
  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  protected def initializeEdge(entityId:String, entityType:String):IgnitePut = {

    val edge = new IgnitePut(entityId, ElementType.EDGE)
    /*
     * The data type is known [String] and synchronized with ValueType
     * note, the provided entity type is used to specify the edge label
     */
    edge.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    edge.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())
    /*
     * Assign time management fields: These fields are internal
     * fields and are not synchronized with potentially existing
     * ones that have the same meaning.
     */
    val timestamp = System.currentTimeMillis()
    edge.addColumn(
      IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)

    edge.addColumn(
      IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

    edge
  }
  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  protected def initializeVertex(entityId:String, entityType:String):IgnitePut = {

    val vertex = new IgnitePut(entityId, ElementType.VERTEX)
    /*
     * The data type is known [String] and synchronized with ValueType;
     * note, the provided entity type is used to specify the vertex label
     */
    vertex.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    vertex.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())
    /*
     * Assign time management fields: These fields are internal
     * fields and are not synchronized with potentially existing
     * ones that have the same meaning.
     */
    val timestamp = System.currentTimeMillis()
    vertex.addColumn(
      IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)

    vertex.addColumn(
      IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

    vertex
  }

  protected def transformHashes(hashes:Any): List[(String,String)] = {

    val result = mutable.HashMap.empty[String, String]
    /*
     * Flatten hashes
     */
    hashes match {
      case _: List[Any] =>
        hashes.asInstanceOf[List[Map[String, String]]].foreach(hash => {
          val k = hash("algorithm")
          val v = hash("hash")

          result += k -> v
        })
      case entries: Map[String, String] =>
        entries.foreach(entry => {
          result += entry._1 -> entry._2
        })
      case _ =>
        val now = new Date().toString
        throw new Exception(s"[ERROR] $now - Unknown data type for hashes detected.")
    }
    result.toList

  }

}
