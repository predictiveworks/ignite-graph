package de.kp.works.ignite.stream.opencti
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.kp.works.ignite.client.mutate.{IgniteMutation, IgnitePut}
import de.kp.works.ignite.stream.opencti.transformer._

import scala.collection.mutable

object CTITransformer {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def transform(sseEvents:Seq[SseEvent]):(Seq[IgniteMutation], Seq[IgniteMutation]) = {

    val vertices = mutable.ArrayBuffer.empty[IgniteMutation]
    val edges    = mutable.ArrayBuffer.empty[IgniteMutation]

    sseEvents.foreach(sseEvent => {
      /*
       * The event type specifies the data operation
       * associated with the event; this implementation
       * currently supports `create`, `delete` and `update`
       * operations
       */
      val event = sseEvent.eventType
      val payload = mapper.readValue(sseEvent.data, classOf[Map[String, Any]])

      val entityId = payload.getOrElse("id", "").asInstanceOf[String]
      val entityType = payload.getOrElse("type", "").asInstanceOf[String]

      if (entityId.isEmpty || entityType.isEmpty) {
        /* Do nothing */
      }
      else {

        val filter = Seq("id", "type")
        val data = payload.filterKeys(k => !filter.contains(k))

        val (v,e) = event match {
          case "create" =>
            transformCreate(entityId, entityType, data)
          case "delete" =>
            transformDelete(entityId, entityType, data)
          case "merge" | "sync" => (None, None)
          case "update" =>
            transformUpdate(entityId, entityType, data)
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Unknown event type detected: $event")
        }

        if (v.isDefined) vertices ++= v.get
        if (e.isDefined) edges    ++= e.get

      }

    })

    (vertices, edges)

  }

  private def transformCreate(entityId:String, entityType:String, data:Map[String, Any]):(Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {

      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.createRelationship(entityId, entityType, data)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return EdgeTransformer.createSighting(entityId, entityType, data)
      }

      // TODO
      throw new Exception("Not implement yet")
    }
    else {

      if (STIX.isCyberObservable(entityType)) {
        VertexTransformer.createObservable(entityId, entityType, data)
      }
      // TODO
      throw new Exception("Not implement yet")
    }
  }

  private def transformDelete(entityId:String, entityType:String, data:Map[String, Any]):(Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      // TODO
      throw new Exception("Not implement yet")
    }
    else {
      // TODO
      throw new Exception("Not implement yet")
    }
  }
  /**
   * Create & update request result in a list of [IgnitePut], but
   * must be processed completely different as OpenCTI leverages
   * a complex `x_opencti_patch` field to specify updates.
   */
  private def transformUpdate(entityId:String, entityType:String, data:Map[String, Any]):(Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      // TODO
      throw new Exception("Not implement yet")
    }
    else {
      // TODO
      throw new Exception("Not implement yet")
    }
  }
}
