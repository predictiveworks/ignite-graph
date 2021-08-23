package de.kp.works.ignite.stream.fiware
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

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.MethodDirectives.post
import akka.http.scaladsl.server.directives.PathDirectives.path
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.stream.fiware.FiwareActor._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
 * The notification endpoint, the Orion Context Broker
 * sends (subscribed) notifications to.
 */
class FiwareServer {

  private var callback:Option[FiwareNotificationCallback] = None

  private var server:Option[Future[Http.ServerBinding]] = None
  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  implicit val system: ActorSystem = ActorSystem(CommonConfig.getSystemName)
  implicit lazy val context: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(5.seconds)
  /**
   * Specify the callback to be used by this server
   * to send Orion notification to the respective
   * Ignite cache.
   *
   * The current implementation leverages the Fiware
   * Streamer as callback
   */
  def setCallback(callback:FiwareNotificationCallback):FiwareServer = {
    this.callback = Some(callback)
    this
  }
  /**
   * This method launches the Orion notification server and also subscribes
   * to the Orion Context Broker for receiving NGSI event notifications
   */
  def launch():Unit = {
    /*
     * The FiwareActor is used to receive NGSI events and delegate
     * them to the provided callback
     */
    if (callback.isEmpty)
      throw new Exception("[FiwareServer] No callback specified to send notifications to.")

    lazy val fiwareActor = system
      .actorOf(Props(new FiwareActor(callback.get)), "FiwareActor")

    def routes:Route = {
      path("notifications") {
        post {
          /*
           * Extract (full) HTTP request from POST notification
           * of the Orion Context Broker
           */
          extractRequest { request =>
            complete {

              val future = fiwareActor ? request
              Await.result(future, timeout.duration) match {
                case Response(Failure(e)) =>
                  /*
                   * A failure response is sent with 500 and
                   * the respective exception message
                   */
                  val message = e.getMessage + "\n"
                  val length = message.getBytes.length

                  val headers = List(
                    `Content-Type`(`text/plain(UTF-8)`),
                    `Content-Length`(length)
                  )

                  HttpResponse(
                    status=StatusCodes.InternalServerError,
                    headers = headers,
                    entity = ByteString(message),
                    protocol = HttpProtocols.`HTTP/1.1`)
                case Response(Success(_)) =>

                  val headers = List(
                    `Content-Type`(`text/plain(UTF-8)`),
                    `Content-Length`(0)
                  )

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

    val bindingCfg = CommonConfig.getFiwareServerBinding
    val (host, port) = (bindingCfg.getString("host"), bindingCfg.getInt("port"))
    /*
     * Distinguish between SSL/TLS and non-SSL/TLS requests
     */
    server = if (FiwareSsl.isServerSsl) {
       /*
        * The request protocol in the notification url must be
        * specified as 'http://'
        */
       Some(Http().bindAndHandle(routes , host, port))

    } else {
      /*
       * The request protocol in the notification url must
       * be specified as 'https://'. In this case, an SSL
       * security context must be specified
       */
      val context = FiwareSsl.buildServerContext
      Some(Http().bindAndHandle(routes, host, port, connectionContext = context))

    }

    /* STEP #3: Register subscriptions with Orion Context Broker */

    val subscriptions = FiwareSubscriptions.getSubscriptions
    subscriptions.foreach(subscription => {

      try {

        val future = FiwareClient.subscribe(subscription, system)
        val response = Await.result(future, timeout.duration)

        val sid = FiwareClient.getSubscriptionId(response)
        FiwareSubscriptions.register(sid, subscription)

      } catch {
        case t:Throwable =>
          /*
           * The current implementation of the Fiware streaming
           * support is an optimistic approach that focuses on
           * those subscriptions that were successfully registered.
           */
          println("[ERROR] ------------------------------------------------")
          println("[ERROR] Registration of subscription failed:")
          println(s"$subscription")
          println("[ERROR] ------------------------------------------------")
      }

    })

  }

  def stop():Unit = {

    if (server.isEmpty)
      throw new Exception("Notification server was not launched.")

    server.get
      /*
       * rigger unbinding from port
       */
      .flatMap(_.unbind())
      /*
       * Shut down application
       */
      .onComplete(_ => {
        system.terminate()
      })

  }
}
