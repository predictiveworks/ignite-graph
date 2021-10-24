package de.kp.works.ignite.streamer.osquery.tls.actor
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.http.scaladsl.model.HttpRequest
import com.google.gson._
import de.kp.works.ignite.streamer.osquery.tls.{TLSEvent, TLSEventHandler}
import de.kp.works.ignite.streamer.osquery.OsqueryConstants
import de.kp.works.ignite.streamer.osquery.tls.actor.StatusActor._
import de.kp.works.ignite.streamer.osquery.tls.db.{DBApi, OsqueryNode}

import scala.collection.JavaConversions._

/*
 * This actor publishes status logs to a pre-configured
 * data sink
 */
class StatusActor(api:DBApi, handler:TLSEventHandler) extends BaseActor(api) {

  override def receive: Receive = {

    case request:StatusReq =>
      /*
       * Send response message to `origin` immediately
       * as the logging task may last some time:
       *
       * LogActor -- [StatusReq] --> StatusActor
       *
       */
      val origin = sender
      origin ! StatusRsp("Logging started", success = true)

      try {

        val batch = buildBatch(request).toString
        /*
         * Send each status message as log event to the
         * output channel; this approach is equivalent
         * to the Fleet based mechanism.
         */
        batch.foreach(batchObj => {

          val event = TLSEvent(eventType = "tls/osquery_status", eventData = batchObj.toString)
          handler.eventArrived(event)

        })


      } catch {
        case t:Throwable => origin ! StatusRsp("Status logging failed: " + t.getLocalizedMessage, success = false)
      }
  }

  private def buildBatch(request:StatusReq):JsonArray = {

    val batch = new JsonArray

    val node = request.node
    val data = request.data.iterator

    while (data.hasNext) {

      val oldObj = data.next.getAsJsonObject
      val batchObj = new JsonObject()
      /*
       * Assign header to event
       */
      batchObj.addProperty(OsqueryConstants.HOST_IDENTIFIER, node.hostIdentifier)
      batchObj.addProperty(OsqueryConstants.NODE_IDENT, node.uuid)
      batchObj.addProperty(OsqueryConstants.NODE_KEY, node.nodeKey)

      /*
       * Assign body to event
       */
      oldObj.entrySet.foreach(entry => {

        val k = entry.getKey
        val v = entry.getValue

        batchObj.add(k, v)

      })

      batch.add(batchObj)

    }

    batch

  }

  override def execute(request: HttpRequest): String = {
    throw new Exception("Not implemented.")
  }

}

object StatusActor {

  case class StatusReq(node:OsqueryNode, data:JsonArray)
  case class StatusRsp(message:String, success:Boolean)

}
