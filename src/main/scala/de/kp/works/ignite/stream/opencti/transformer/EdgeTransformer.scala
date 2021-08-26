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
import de.kp.works.ignitegraph.{ElementType, IgniteConstants}

import java.util.Date
import scala.collection.mutable

object EdgeTransformer extends BaseTransformer {
  /**
   * OpenCTI publishes this event object with basic fields
   * of the internal `from` object and a `to` object or
   * reference
   */
  def createMetaRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val edge = initializeEdge(entityId, entityType)

    /** FROM
     *
     * The basic fields contain `source_ref` and also
     * `x_opencti_source_ref`. It is expected that one
     * of these fields contains the `from` identifier.
     */
    val fromId =
      if (data.contains("source_ref")) {
        data("source_ref").asInstanceOf[String]
      }
      else if (data.contains("x_opencti_source_ref")) {
        /*
         * This is a fallback approach, but should not
         * happen in an OpenCTI event stream
         */
        data("x_opencti_source_ref").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `from` identifier.")
      }

    edge.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

    val `type` = entityType.toLowerCase
    /** TO
     *
     * The extraction of the `to` identifier must distinguish
     * between internal and external meta relationships
     */
    if (STIX.isStixInternalMetaRelationship(`type`)) {
      val toField = `type`.replace("-", "_") + "s"
      val toId = if (data.contains(toField)) {
        /*
         * See stix.js : The STIX internal meta relationship
         * specifies the `to` field as List of a single TO
         * object
         */
        val value = data(toField).asInstanceOf[List[Map[String,Any]]].head
        value("id").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `to` identifier.")
      }

      edge.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)

    }
    else {
      val toId = if (`type` != STIX.RELATION_CREATED_BY) {
        val toField = `type`.replace("-", "_") + "s"
        if (data.contains(toField)) {
          /*
           * See stix.js : The STIX external meta relationship
           * specifies the `to` field as List of a single identifier
           */
          data(toField).asInstanceOf[List[String]].head
        }
        else {
          val now = new java.util.Date().toString
          throw new Exception(s"[ERROR] $now - Relationship does not contain a `to` identifier.")
        }
      }
      else {
        val toField = `type`.replace("-", "_")
        if (data.contains(toField)) {
          data(toField).asInstanceOf[String]
        }
        else {
          val now = new java.util.Date().toString
          throw new Exception(s"[ERROR] $now - Relationship does not contain a `to` identifier.")
        }
      }

      edge.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)

    }

    (None, Some(Seq(edge)))
  }
  /**
   * OpenCTI publishes this event object with basic fields
   * of the internal `from` object and a `to` reference
   * that is derived from entity_type
   */
  def createObservableRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val edge = initializeEdge(entityId, entityType)

    /** FROM
     *
     * The basic fields contain `source_ref` and also
     * `x_opencti_source_ref`. It is expected that one
     * of these fields contains the `from` identifier.
     */
    val fromId =
      if (data.contains("source_ref")) {
        data("source_ref").asInstanceOf[String]
      }
      else if (data.contains("x_opencti_source_ref")) {
        /*
         * This is a fallback approach, but should not
         * happen in an OpenCTI event stream
         */
        data("x_opencti_source_ref").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `from` identifier.")
      }

    edge.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

    /** TO **/

    val toField = entityType.replace("-", "_") + "_ref"
    val toId =
      if (data.contains(toField)) {
        data(toField).asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `to` identifier.")
      }

    edge.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)
    (None, Some(Seq(edge)))

  }
  def createRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val edge = initializeEdge(entityId, entityType)
    var filteredData = data

    /** FROM
     *
     * The reference, source_ref or x_opencti_source_ref,
     * contains the ID of the (from) SDO.
     *
     * This implementation defines `Relation` as an [Edge] that
     * points from the SDO to another one.
     *
     * In contrast to `Sighting`, OpenCTI leverages [String]
     * instead of a List[String]
     */
    val fromId =
      if (data.contains("source_ref")) {
        data("source_ref").asInstanceOf[String]
      }
      else if (data.contains("x_opencti_source_ref")) {
        /*
         * This is a fallback approach, but should not
         * happen in an OpenCTI event stream
         */
        data("x_opencti_source_ref").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `from` identifier.")
      }

    edge.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

    /** TO
     *
     * In contrast to `Sighting`, OpenCTI leverages [String]
     * instead of a List[String]
     */
    val toId =
      if (data.contains("target_ref")) {
        data("target_ref").asInstanceOf[String]
      }
      else if (data.contains("x_opencti_target_ref")) {
        /*
         * This is a fallback approach, but should not
         * happen in an OpenCTI event stream
         */
        data("x_opencti_target_ref").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Relationship does not contain a `to` identifier.")
      }

    edge.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)

    val filter = Seq(
      "source_ref",
      "target_ref",
      "x_opencti_source_ref",
      "x_opencti_target_ref")

    filteredData = filteredData.filterKeys(k => !filter.contains(k))
    /*
     * The following relationship attributes are added as [Edge] properties
     */
    val fields = Seq("description", "name")
    fields.foreach(field => {

      if (filteredData.contains(field)) {
        val propKey  = field
        val (propType, propValu) = ("STRING", data(field).asInstanceOf[String])

        edge.addColumn(propKey, propType, propValu)

      }

    })

    (None, Some(Seq(edge)))

  }
  /**
   * This method transforms a STIX v2.1 `Sighting` into an [Edge]
   * that connects a STIX Domain Object or Cyber Observable with
   * an instance that recognized this object or observable.
   */
  def createSighting(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val edge = initializeEdge(entityId, entityType)
    var filteredData = data

    /** FROM
     *
     * The reference, sighting_of_ref, contains the ID of the SDO
     * that was sighted, which e.g. can be an indicator or cyber
     * observable.
     *
     * This implementation defines `Sighting` as an [Edge] that
     * points from the SDO to the (identity) that also sighted
     * the indicator or cyber observable.
     */
    val fromId =
      if (data.contains("sighting_of_ref")) {
        data("sighting_of_ref").asInstanceOf[String]
      }
      else if (data.contains("x_opencti_sighting_of_ref")) {
        data("x_opencti_sighting_of_ref").asInstanceOf[String]
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Sighting does not contain a `from` identifier.")
      }

    edge.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

    /** TO **/

    val toId =
      if (data.contains("where_sighted_refs")) {
        /*
         * OpenCTI specifies the `from` identifier as [List],
         * with a single list element
         */
        val ids = data("where_sighted_refs").asInstanceOf[List[String]]
        if (ids.isEmpty) {
          val now = new java.util.Date().toString
          throw new Exception(s"[ERROR] $now - Sighting does not contain a `to` identifier.")
        }
        else
          ids.head
      }
      else {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Sighting does not contain a `to` identifier.")
      }

    edge.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)

    val filter = Seq(
      "sighting_of_ref",
      "where_sighted_refs",
      "x_opencti_sighting_of_ref",
      "x_opencti_where_sighted_refs")

    filteredData = filteredData.filterKeys(k => !filter.contains(k))
    /*
     * The following sighting attributes are added as [Edge] properties
     */
    val fields = Seq(
      "attribute_count",
      "confidence",
      "count",
      "created",
      "created_by_ref",
      "description",
      "first_seen",
      "last_seen",
      "modified",
      "name")

    fields.foreach(field => {

      if (filteredData.contains(field)) {
        val propKey  = field
        val (propType, propValu) = field match {
          case "attribute_count" | "count" | "confidence" =>
            ("INT", data(field).asInstanceOf[Int].toString)
          case _ =>
            ("STRING", data(field).asInstanceOf[String])
        }

        edge.addColumn(propKey, propType, propValu)

      }

    })

    (None, Some(Seq(edge)))

  }
  /**
   * This method deletes an [Edge] object from the respective
   * Ignite cache. Removing selected edge properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteMetaRelationship(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.EDGE)
    (None, Some(Seq(delete)))
  }
  /**
   * This method deletes an [Edge] object from the respective
   * Ignite cache. Removing selected edge properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteObservableRelationship(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.EDGE)
    (None, Some(Seq(delete)))
  }
  /**
   * This method deletes an [Edge] object from the respective
   * Ignite cache. Removing selected edge properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteRelationship(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.EDGE)
    (None, Some(Seq(delete)))
  }
  /**
   * This method deletes an [Edge] object from the respective
   * Ignite cache. Removing selected edge properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteSighting(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.EDGE)
    (None, Some(Seq(delete)))
  }

  def updateMetaRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    // TODO
    throw new Exception("Not implemented yet")
  }

  def updateObservableRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    // TODO
    throw new Exception("Not implemented yet")
  }

  def updateRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    // TODO
    throw new Exception("Not implemented yet")
  }

  def updateSighting(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    // TODO
    throw new Exception("Not implemented yet")
  }

  /**
   * This method creates an [Edge] object for an SDO
   * to describe the relationship to the `Identity`
   * that created the object
   */
  def createCreatedBy(entityId:String, data:Map[String, Any]):Option[Seq[IgnitePut]] = {

    val createdBy = data("created_by_ref")
    try {
      /*
       * The identifier of the respective edge is system
       * generated
       */
      val edgeId = s"created-by-${java.util.UUID.randomUUID.toString}"
      val edge = initializeEdge(edgeId, HAS_CREATED_BY)

      /* FROM */
      edge.addColumn(
        IgniteConstants.FROM_COL_NAME, "STRING", entityId)

      /* TO
       *
       * OpenCTI supports two different formats to describe
       * the created_by_ref field:
       *
       * (1) [String]: identifier
       *
       * (2) [Map[String,String]: 'reference', 'value', 'x_opencti_internal_id'
       *
       * In both cases, an identifier is provided to reference
       * the creator (identity object)
       */
      createdBy match {
        case value: Map[String, Any] =>
          edge.addColumn(
            IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])

        case value: String =>
          edge.addColumn(
            IgniteConstants.TO_COL_NAME, "STRING", value)

        case _ =>
          val now = new Date().toString
          throw new Exception(s"[ERROR] $now - The data type of the created_by field is not supported.")
      }

      Some(Seq(edge))

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating created_by failed: ", t)
        None
    }

  }

  def createKillChainPhases(entityId:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges = mutable.ArrayBuffer.empty[IgnitePut]

    try {

      val killChainPhases = data("kill_chain_phases").asInstanceOf[List[Any]]
      killChainPhases.foreach(killChainPhase => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val edgeId = s"external-reference-${java.util.UUID.randomUUID.toString}"
        val edge = initializeEdge(edgeId, HAS_KILL_CHAIN_PHASE)

        /* FROM */
        edge.addColumn(
          IgniteConstants.FROM_COL_NAME, "STRING", entityId)

        /* TO
         *
         * OpenCTI supports two different formats to describe
         * kill chain phases:
         *
         * (1) fields: 'kill_chain_name', 'phase_name'
         *
         * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
         */

        killChainPhase match {
          case value: Map[String, Any] =>
            if (value.contains("value")) {
              /*
               * In this case, the processing of a kill chain phase is
               * restricted to create an [Edge]; the `value` field is
               * the identifier of the Kill Chain object
               */
              edge.addColumn(
                IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])
              /*
               * The `reference` field specifies the kill chain name
               */
              edge.addColumn(
                "kill_chain_name", "STRING", value("reference").asInstanceOf[String])
              /*
               * Assign created [Edge] to the list of edges
               */
              edges += edge
            }
            else {
              /*
               * In this case, the processing of the kill chain phase demands
               * to create the kill chain phase as [Vertex]
               */
              val vertexId = s"kill-chain-phase-${java.util.UUID.randomUUID.toString}"
              /*
               * The system generated vertex identifier is used as `TO` identifier
               */
              edge.addColumn(
                IgniteConstants.TO_COL_NAME, "STRING", vertexId)

              val vertex = initializeVertex(vertexId, KILL_CHAIN_PHASE)

              /* kill_chain_name */
              val kill_chain_name = data.getOrElse("kill_chain_name", "").asInstanceOf[String]
              vertex.addColumn("kill_chain_name", "STRING", kill_chain_name)

              /* phase_name */
              val phase_name = data.getOrElse("phase_name", "").asInstanceOf[String]
              vertex.addColumn("phase_name", "STRING", phase_name)
              /*
               * Assign created [Vertex] and [Edge] to the list of
               * vertices and edges
               */
              vertices += vertex
              edges    += edge
            }

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided kill chain phase is not supported.")
        }

      })

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating external references failed: ", t)
    }

    (Some(vertices), Some(edges))

  }

  def createObjectLabels(entityId:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges = mutable.ArrayBuffer.empty[IgnitePut]

    try {

      val labels = data("labels").asInstanceOf[List[Any]]
      labels.foreach(label => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val edgeId = s"object-label-${java.util.UUID.randomUUID.toString}"
        val edge = initializeEdge(edgeId, HAS_OBJECT_LABEL)

        /* FROM */
        edge.addColumn(
          IgniteConstants.FROM_COL_NAME, "STRING", entityId)

        /* TO
         *
         * OpenCTI supports two different formats to describe
         * object labels:
         *
         * (1) [String]: the label value
         *
         * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        label match {
          case value: Map[String, Any] =>
            /*
             * In this case, the processing of an object label is
             * restricted to create an [Edge]; the `value` field
             * contains the identifier of the Object Label
             */
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])
            /*
             * The `reference` field specifies the label of the
             * Object Label
             */
            edge.addColumn(
              "object_label", "STRING", value("reference").asInstanceOf[String])
            /*
             * Assign created [Edge] to the list of edges
             */
            edges += edge
          case value:String =>
            /*
             * In this case, the processing of the Object Label demands
             * to create the Object Label as [Vertex]
             */
            val vertexId = s"object-label-${java.util.UUID.randomUUID.toString}"
            /*
             * The system generated vertex identifier is used as `TO` identifier
             */
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", vertexId)

            val vertex = initializeVertex(vertexId, OBJECT_LABEL)

            /* object_label */
            vertex.addColumn("object_label", "STRING", value)
            /*
             * Assign created [Vertex] and [Edge] to the list of
             * vertices and edges
             */
            vertices += vertex
            edges    += edge

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided object label is not supported.")
        }

      })

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating object labels failed: ", t)
    }

    (Some(vertices), Some(edges))
  }

  /**
   * This method creates edges between a STIX domain object
   * or cyber observable and assigned `object_marking_refs`.
   */
  def createObjectMarkings(entityId:String, data:Map[String, Any]):Option[Seq[IgnitePut]] = {

    val markings = data("object_marking_refs").asInstanceOf[List[Any]]

    val edges = try {
      markings.map(marking => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val id = s"object-marking-${java.util.UUID.randomUUID.toString}"
        val edge = initializeEdge(id, HAS_OBJECT_MARKING)

        /* FROM */
        edge.addColumn(
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
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value)

          case value: Map[String, Any] =>
            /*
             * The `value` of the provided Map refers to the
             * Object Marking object (see OpenCTI data model).
             */
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided object marking is not supported.")
        }
        /*
         * This internal edge does not contain any further properties
         * and is just used to connect and SDO and its Object Markings.
         */
        edge
      })

    } catch {
      case t:Throwable =>
        LOGGER.error("Creating object markings failed: ", t)
        Seq.empty[IgnitePut]
    }

    Some(edges)

  }

  def createObjectReferences(entityId:String, data:Map[String, Any]):Option[Seq[IgnitePut]] = {

    val references = data("object_refs").asInstanceOf[List[Any]]
    val edges = try {
      references.map(reference => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val id = s"object-reference-${java.util.UUID.randomUUID.toString}"
        val edge = initializeEdge(id, HAS_OBJECT_REFERENCE)

        /* FROM */
        edge.addColumn(
          IgniteConstants.FROM_COL_NAME, "STRING", entityId)

        /* TO
         *
         * OpenCTI ships with two different formats to describe
         * an object references:
         *
         * (1) List[String] - identifiers
         *
         * (2) List[Map[String, String]]
         *     fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        reference match {
          case value: String =>
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value)

          case value: Map[String, Any] =>
            /*
             * The `value` of the provided Map refers to the
             * Object Marking object (see OpenCTI data model).
             */
            edge.addColumn(
              IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided object reference is not supported.")
        }
        /*
         * This internal edge does not contain any further properties
         * and is just used to connect and SDO and its Object Reference.
         */
        edge
      })

    } catch {
      case t:Throwable =>
        LOGGER.error("Creating object references failed: ", t)
        Seq.empty[IgnitePut]
    }

    Some(edges)

  }

  def createExternalReferences(entityId:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges = mutable.ArrayBuffer.empty[IgnitePut]

    try {

      val references = data("external_references").asInstanceOf[List[Any]]
      references.foreach(reference => {
        /*
         * The identifier of the respective edge is system
         * generated
         */
        val edgeId = s"external-reference-${java.util.UUID.randomUUID.toString}"
        val edge = initializeEdge(edgeId, HAS_EXTERNAL_REFERENCE)

        /* FROM */
        edge.addColumn(
          IgniteConstants.FROM_COL_NAME, "STRING", entityId)

        /* TO
         *
         * OpenCTI supports two different formats to describe
         * external references:
         *
         * (1) fields: 'source_name', 'description', 'url', 'hashes', 'external_id'
         *
         * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        reference match {
          case value: Map[String, Any] =>
            if (value.contains("value")) {
              /*
               * In this case, the processing of an external reference is
               * restricted to create an [Edge]; the `value` field contains
               * the identifier of the External Reference object
               */
              edge.addColumn(
                IgniteConstants.TO_COL_NAME, "STRING", value("value").asInstanceOf[String])
              /*
               * The `reference` field specifies the source name of the
               * external object
               */
              edge.addColumn(
                "source_name", "STRING", value("reference").asInstanceOf[String])
              /*
               * Assign created [Edge] to the list of edges
               */
              edges += edge
            }
            else {
              /*
               * In this case, the processing of the external reference demands
               * to create the external reference as [Vertex]
               */
              val vertexId = s"external-reference-${java.util.UUID.randomUUID.toString}"
              /*
               * The system generated vertex identifier is used as `TO` identifier
               */
              edge.addColumn(
                IgniteConstants.TO_COL_NAME, "STRING", vertexId)

              val vertex = initializeVertex(vertexId, EXTERNAL_REFERENCE)

              /* source_name */
              val source_name = data.getOrElse("source_name", "").asInstanceOf[String]
              vertex.addColumn("source_name", "STRING", source_name)

              /* description */
              val description = data.getOrElse("description", "").asInstanceOf[String]
              vertex.addColumn("description", "STRING", description)

              /* url */
              val url = data.getOrElse("url", "").asInstanceOf[String]
              vertex.addColumn("url", "STRING", url)

              /* external_id */
              val external_id = data.getOrElse("external_id", "").asInstanceOf[String]
              vertex.addColumn("external_id", "STRING", external_id)

              /*
               * hashes is optional, but when provided specifies a dictionary of hashes
               * for the contents of the url:
               * hashes: {
               *  "SHA-256": "..."
               * }
               */
              if (data.contains("hashes")) {
                val hashes = transformHashes(data("hashes"))
                hashes.foreach{case (k,v) =>
                  vertex.addColumn(k, "STRING", v)
                }
              }
              /*
               * Assign created [Vertex] and [Edge] to the list of
               * vertices and edges
               */
              vertices += vertex
              edges    += edge
            }
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided external reference is not supported.")
        }

      })

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating external references failed: ", t)
    }

    (Some(vertices), Some(edges))
  }

}
