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

object EdgeTransformer {
  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  private def initializePut(entityId:String, entityType:String):IgnitePut = {

    val put = new IgnitePut(entityId, ElementType.EDGE)
    /*
     * The data type is known [String] and synchronized with ValueType
     * note, the provided entity type is used to specify the edge label
     */
    put.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    put.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())

    put
  }

  def createRelationship(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val put = initializePut(entityId, entityType)
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

    put.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

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

    put.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)
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

        put.addColumn(propKey, propType, propValu)

      }

    })

    (None, Some(Seq(put)))

  }
  /**
   * This method transforms a STIX v2.1 `Sighting` into an [Edge]
   * that connects a STIX Domain Object or Cyber Observable with
   * an instance that recognized this object or observable.
   */
  def createSighting(entityId:String, entityType:String, data:Map[String,Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {

    val put = initializePut(entityId, entityType)
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

    put.addColumn(IgniteConstants.FROM_COL_NAME, "STRING", fromId)

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

    put.addColumn(IgniteConstants.TO_COL_NAME, "STRING", toId)
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

        put.addColumn(propKey, propType, propValu)

      }

    })

    (None, Some(Seq(put)))

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

}
