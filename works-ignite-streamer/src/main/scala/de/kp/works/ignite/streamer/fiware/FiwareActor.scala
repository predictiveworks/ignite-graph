package de.kp.works.ignite.streamer.fiware
/*
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

import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy}
import akka.actor.SupervisorStrategy._
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.google.gson._
import de.kp.works.ignite.conf.WorksConf

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Try

abstract class BaseActor extends Actor with ActorLogging {
  /**
   * The header positions that hold service and
   * service path information
   */
  val SERVICE_HEADER = 4
  val SERVICE_PATH_HEADER = 5
  /**
   * The actor system is implicitly accompanied by a materializer,
   * and this materializer is required to retrieve the bytestring
   */
  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val conf = WorksConf.getFiwareActorCfg

  implicit val timeout: Timeout = {
    val value = conf.getInt("timeout")
    Timeout(value.seconds)
  }
  /**
   * Parameters to control the handling of failed child actors:
   * it is the number of retries within a certain time window.
   *
   * The supervisor strategy restarts a child up to 10 restarts
   * per minute. The child actor is stopped if the restart count
   * exceeds maxNrOfRetries during the withinTimeRange duration.
   */
  protected val maxRetries: Int = conf.getInt("maxRetries")
  protected val timeRange: FiniteDuration = {
    val value = conf.getInt("timeRange")
    value.minute
  }
  /**
   * Child actors are defined leveraging a RoundRobin pool with a
   * dynamic resizer. The boundaries of the resizer are defined
   * below
   */
  protected val lower: Int = conf.getInt("lower")
  protected val upper: Int = conf.getInt("upper")
  /**
   * The number of instances for the RoundRobin pool
   */
  protected val instances: Int = conf.getInt("instances")
  /**
   * Each actor is the supervisor of its children, and as such each
   * actor defines fault handling supervisor strategy. This strategy
   * cannot be changed afterwards as it is an integral part of the
   * actor system’s structure.
   */
  override val supervisorStrategy: OneForOneStrategy =
    /*
     * The implemented supervisor strategy treats each child separately
     * (one-for-one). Alternatives are, e.g. all-for-one.
     *
     */
    OneForOneStrategy(maxNrOfRetries = maxRetries, withinTimeRange = timeRange) {
      case _: ArithmeticException      => Resume
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }
  /**
   * Method to unpack the (incoming) Orion Broker notification
   * request and to transform into an internal JSON format for
   * further processing.
   */
  def toFiwareNotification(request:HttpRequest):FiwareEvent = {

    /** HEADERS **/

    val headers = request.headers

    val service = headers(SERVICE_HEADER).value()
    val servicePath = headers(SERVICE_PATH_HEADER).value()

    /** BODY **/

    /* Extract body as String from request entity */

    val source = request.entity.dataBytes
    val future = source.runFold(ByteString(""))(_ ++ _)
    /*
     * We do not expect to retrieve large messages
     * and accept a blocking wait
     */
    val bytes = Await.result(future, timeout.duration)
    val body = bytes.decodeString("UTF-8")

    /* We expect that the Orion Context Broker sends a JSON object */
    val payload = JsonParser.parseString(body).getAsJsonObject
    FiwareEvent(service, servicePath, payload)

  }

}
/**
 * The [FiwareActor] is responsible for executing HTTP based
 * notification requests from the Orion Broker. Each notification
 * is checked whether it refers to a registered subscriptions,
 * and, if this is the case, further processing is delegated to
 * the Ignite (Fiware) Streamer
 */
class FiwareActor(callback:FiwareEventHandler) extends BaseActor {

  import FiwareActor._

  override def receive: Receive = {

    case request: HttpRequest =>
      sender ! Response(Try({
        execute(request)
      })
        .recover {
          case e: Exception =>
            throw new Exception(e.getMessage)
        })

  }
  /**
   * Method to transform an Orion Broker notification
   * message into an internal format and to delegate
   * further processing to the provided callback
   */
  private def execute(request: HttpRequest):Unit = {
    /*
     * Convert Http request from Orion Broker
     * into internal notification format
     */
    val notification = toFiwareNotification(request)
    /*
     * Before we continue to delegate the notification
     * to the Ignite streamer, we check whether the
     * notification refers to a registered subscription.
     */
    val json = notification.payload
    /*
     * {
        "data": [
            {
                "id": "Room1",
                "temperature": {
                    "metadata": {},
                    "type": "Float",
                    "value": 28.5
                },
                "type": "Room"
            }
        ],
        "subscriptionId": "57458eb60962ef754e7c0998"
       }
     */
    val sid = json.get("subscriptionId").getAsString
    if (FiwareSubscriptions.isRegistered(sid))
      callback.notificationArrived(notification)

  }
}

object FiwareActor {

  case class Response(status: Try[_])

}