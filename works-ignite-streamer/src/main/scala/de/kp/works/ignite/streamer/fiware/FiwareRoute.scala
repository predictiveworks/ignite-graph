package de.kp.works.ignite.streamer.fiware

/**
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ContentTypes.`text/plain(UTF-8)`
import akka.http.scaladsl.model.{HttpProtocols, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.{`Content-Length`, `Content-Type`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.post
import akka.http.scaladsl.server.directives.PathDirectives.path
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import de.kp.works.ignite.streamer.fiware.actors.NotifyActor.Response

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

object FiwareRoute {

  val NOTIFY_ACTOR = "notify_actor"

}

class FiwareRoute(actors:Map[String, ActorRef])(implicit system:ActorSystem) {

  import FiwareRoute._

  implicit lazy val context: ExecutionContextExecutor = system.dispatcher
  /**
   * Common timeout for all Akka connections
   */
  val duration: FiniteDuration = 15.seconds
  implicit val timeout: Timeout = Timeout(duration)

  def buildRoute:Route = notifyRoute
  /**
   * The input channel that retrieves `notification`
   * requests from the Fiware Context Broker as a
   * response to a previous subscription
   */
  def notifyRoute:Route = {

    val actor = actors(NOTIFY_ACTOR)
    path("v2" / "notify") {
      post {
        /*
         * Extract (full) HTTP request from POST notification
         * of the Orion Context Broker
         */
        extractRequest { request =>
          complete {

            val future = actor ? request
            Await.result(future, timeout.duration) match {
              case Response(Failure(e)) =>
                /*
                 * A failure response is sent with 500 and
                 * the respective exception message
                 */
                val message = e.getMessage + "\n"
                val length = message.getBytes.length

                val headers = getTextHeaders(length)

                HttpResponse(
                  status=StatusCodes.InternalServerError,
                  headers = headers,
                  entity = ByteString(message),
                  protocol = HttpProtocols.`HTTP/1.1`)
              case Response(Success(_)) =>

                val headers = getTextHeaders(0)
                HttpResponse(
                  status=StatusCodes.OK,
                  headers = headers,
                  entity = ByteString(),
                  protocol = HttpProtocols.`HTTP/1.1`)
            }
          }
        }
      }
    }
  }
  /**
   * Helper method to build plain text headers
   * for `notify` response headers to the Fiware
   * Context Broker
   */
  private def getTextHeaders(length:Int) = {
    val headers = List(
      `Content-Type`(`text/plain(UTF-8)`),
      `Content-Length`(length)
    )
    headers
  }

}
