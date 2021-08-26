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

import de.kp.works.ignite.client.mutate.{IgniteDelete, IgnitePut}
import de.kp.works.ignitegraph.ElementType

import scala.collection.mutable

object VertexTransformer extends BaseTransformer {
  /**
   * A STIX object is either a STIX Domain Object (SDO)
   * or a STIX Cyber Observable (SCO)
   */
  def createStixObject(entityId:String, entityType:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * Associated (internal) vertices & edges
     */
    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges    = mutable.ArrayBuffer.empty[IgnitePut]

    val vertex = initializeVertex(entityId, entityType, "create")
    var filteredData = data

    /*
     * The remaining part of this method distinguishes between fields
     * that describe describe relations and those that carry object
     * properties.
     */

    /** CREATED BY **/

    if (filteredData.contains("created_by_ref")) {
      /*
       * Creator is transformed to an edge
       */
      val e = EdgeTransformer.createCreatedBy(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'created_by_ref' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "created_by_ref"))

    }

    /** EXTERNAL REFERENCES **/

    if (filteredData.contains("external_references")) {
      /*
       * External references are mapped onto vertices
       * and edges
       */
      val (v, e) = EdgeTransformer.createExternalReferences(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get

      filteredData = filteredData.filterKeys(k => !(k == "external_references"))
    }

    /** KILL CHAIN PHASES
     *
     * This refers to Attack-Pattern, Indicator, Malware and Tools
     */
    if (filteredData.contains("kill_chain_phases")) {
      /*
       * Kill chain phases are mapped onto vertices
       * and edges
       */
      val (v, e) = EdgeTransformer.createKillChainPhases(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get
      /*
       * Remove 'kill_chain_phases' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "kill_chain_phases"))
    }

    /** OBJECT LABELS
     *
     * Create edges from the current SDO to the Object Label;
     * if no Object Label is referenced, a respective vertex
     * is created
     */
    if (filteredData.contains("labels")) {
      /*
       * Object labels are mapped onto vertices and edges
       */
      val (v, e) = EdgeTransformer.createObjectLabels(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get
      /*
       * Remove 'labels' from the provided dataset to restrict
       * further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "labels"))

    }

    /** OBJECT MARKINGS
     *
     * Create edges from the current SDO to the referenced
     * marking definitions
     */
    if (filteredData.contains("object_marking_refs")) {
      /*
       * Object markings are transformed (in contrast to external
       * reference) to edges only
       */
      val e = EdgeTransformer.createObjectMarkings(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'object_marking_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_marking_refs"))
    }

    /** OBJECT REFERENCES **/

    if (filteredData.contains("object_refs")) {
      /*
       * Object references are transformed (in contrast to external
       * reference) to edges only
       */
      val e = EdgeTransformer.createObjectReferences(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'object_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_refs"))
    }

    /** HASHES **/

    if (filteredData.contains("hashes")) {
      val hashes = transformHashes(data("hashes"))
      hashes.foreach{case (k,v) =>
        vertex.addColumn(k, "STRING", v)
      }
      filteredData = filteredData.filterKeys(k => !(k == "hashes"))
    }
    /*
     * Add remaining properties to the SDO or SCO; the current
     * implementation accepts properties of a basic data type
     * or a list where the components specify basic data types.
     */
    filteredData.keySet.foreach(propKey => {

      val value = filteredData(propKey)
      value match {
        case values: List[Any] =>
          try {

            val basicType = getBasicType(values.head)
            putValues(propKey, basicType, values, vertex)

          } catch {
            case _:Throwable => /* Do nothing */
          }

        case _ =>
          try {

            val propType = getBasicType(value)
            putValue(propKey, propType, value, vertex)

          } catch {
            case _:Throwable => /* Do nothing */
          }
      }

    })

    vertices += vertex
    (Some(vertices), Some(edges))
  }
  /**
   * This method deletes a [Vertex] object from the respective
   * Ignite cache. Removing selected vertex properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteStixObject(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.EDGE)
    (None, Some(Seq(delete)))
  }

  def updateStixObject(entityId:String, entityType:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * Associated (internal) vertices & edges
     */
    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges    = mutable.ArrayBuffer.empty[IgnitePut]

    val vertex = initializeVertex(entityId, entityType, "update")
     /*
     * Retrieve patch data from data
     */
    val patch = getPatch(data)
    if (patch.isDefined) {
      // TODO
      throw new Exception("Not implemented yet.")

      (Some(vertices), Some(edges))

    }
    else {
      (None, None)
    }
  }

}
