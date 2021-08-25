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
import de.kp.works.ignite.stream.opencti.CTIProcessor
import de.kp.works.ignitegraph.{ElementType, IgniteConstants}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.collection.mutable

object VertexTransformer {

  private val LOGGER = LoggerFactory.getLogger(classOf[CTIProcessor])

  /**
   * INTERNAL EDGE LABELS
   */
  val HAS_OBJECT_MARKING:String = "has-object-marking"

  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  private def initializePut(entityId:String, entityType:String):IgnitePut = {

    val put = new IgnitePut(entityId, ElementType.VERTEX)
    /*
     * The data type is known [String] and synchronized with ValueType;
     * note, the provided entity type is used to specify the vertex label
     */
    put.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    put.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())

    put
  }

  def createDomainObject(entityId:String, entityType:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * Associated (internal) edges
     */
    val edges = mutable.ArrayBuffer.empty[IgnitePut]

    val put = initializePut(entityId, entityType)
    var filteredData = data
    /*
     * Assign time management fields: These fields are internal
     * fields and are not synchronized with potentially existing
     * ones that have the same meaning.
     */
    val timestamp = System.currentTimeMillis()
    put.addColumn(
      IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)

    put.addColumn(
      IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

    /*
     * The remaining part of this method distinguishes between fields
     * that describe describe relations and those that carry object
     * properties.
     */

    /** EXTERNAL REFERENCES **/

    if (filteredData.contains("external_references")) {
      val externalReferences = filteredData("external_references").asInstanceOf[List[Any]]
      /*
       * OpenCTI supports two different formats to describe
       * external references:
       *
       * (1) fields: 'source_name', 'description', 'url', 'hashes', 'external_id'
       *
       * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
       */
      // TODO
      /*
       * Remove 'external_references' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "external_references"))
    }

    /** KILL CHAIN PHASES **/

    if (filteredData.contains("kill_chain_phases")) {
      val killChainPhases = filteredData("kill_chain_phases").asInstanceOf[List[Any]]
      /*
       * OpenCTI supports two different formats to describe
       * kill chain phases:
       *
       * (1) fields: 'kill_chain_name', 'phase_name'
       *
       * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
       */
      // TODO
      /*
       * Remove 'kill_chain_phases' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "kill_chain_phases"))
    }

    /** OBJECT MARKINGS
     *
     * Create edges from the current SDO to the referenced
     * marking definitions
     */
    if (filteredData.contains("object_marking_refs")) {
      val e = createObjectMarkings(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'object_marking_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_marking_refs"))
    }

    /** OBJECT REFERENCES **/

    if (filteredData.contains("object_refs")) {
      val objectReferences = filteredData("object_refs").asInstanceOf[List[Any]]
      /*
       * OpenCTI supports two different formats to describe
       * kill chain phases:
       *
       * (1) List[String] - identifiers
       *
       * (2) List[Map[String, String]]
       *     fields: 'reference', 'value', 'x_opencti_internal_id'
       */
      // TODO
      /*
       * Remove 'object_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_refs"))
    }

    /*
     * Add properties to the SDO
     */
    // TODO

    (Some(Seq(put)), Some(edges.toSeq))
  }
  /**
   * This method creates edges between a STIX domain object
   * or cyber observable and assigned `object_marking_refs`.
   */
  private def createObjectMarkings(entityId:String, data:Map[String, Any]):Option[Seq[IgnitePut]] = {

    val markings = data("object_marking_refs").asInstanceOf[List[Any]]
    val edges = try {
      markings.map(marking => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val id = s"object-marking-${java.util.UUID.randomUUID.toString}"
        val put = new IgnitePut(id, ElementType.EDGE)
        /*
         * The data type is known [String] and synchronized with ValueType
         */
        put.addColumn(
          IgniteConstants.ID_COL_NAME, "STRING", id)

        put.addColumn(
          IgniteConstants.LABEL_COL_NAME, "STRING", HAS_OBJECT_MARKING)

        /* FROM */
        put.addColumn(
          IgniteConstants.FROM_COL_NAME, "STRING", entityId)

        /* TO
         *
         * OpenCTI ships with two different formats to describe
         * an object marking:
         *
         * (1) List[String] - identifiers
         *
         * (2) List[Map[String, String]]
         *     fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        marking match {
          case value: String =>
            put.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value)

          case value: Map[String, Any] =>
            /*
             * The `value` of the provided Map refers to the
             * Object Marking object (see OpenCTI data model).
             */
            put.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])

          case _ =>
            val now = new Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided object marking is not supported.")
        }
        /*
         * Assign time management fields: These fields are internal
         * fields and are not synchronized with potentially existing
         * ones that have the same meaning.
         */
        val timestamp = System.currentTimeMillis()
        put.addColumn(
          IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)

        put.addColumn(
          IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

        /*
         * This internal edge does not contain any further properties
         * and is just used to connect and SDO and its Object Markings.
         */
        put
      })

    } catch {
      case t:Throwable =>
        LOGGER.error("Creating object markings failed: ", t)
        Seq.empty[IgnitePut]
    }

    Some(edges)

  }

  def createObservable(entityId:String, entityType:String, data:Map[String, Any]):Unit = {
  }

}
