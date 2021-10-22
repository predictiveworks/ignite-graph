package de.kp.works.ignite.streamer.opencti
/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.ssl.SslOptions
import okhttp3.Response
import okhttp3.sse.{EventSource, EventSourceListener, EventSources}

/**
 * The [CTIConnector] connects & listens to the OpenCTI
 * events stream via an SSE client and write these events
 * to the provided output handler.
 */
class CTIConnector(
  endpoint: String,
  callback: CTIEventHandler,
  authToken: Option[String] = None,
  sslOptions: Option[SslOptions] = None) {

  def stop(): Unit = {
    /* Do nothing */
  }

  def start() {

    val sseClient = new SseClient(endpoint, authToken, sslOptions)

    val request = sseClient.getRequest
    val httpClient = sseClient.getHttpClient

    /** SSE **/

    val factory = EventSources.createFactory(httpClient)
    val listener = new EventSourceListener() {

      override def onOpen(eventSource:EventSource, response:Response):Unit = {
        /* Do nothing */
      }
      /*
       * Events format
       *
       * The events published by OpenCTI are based on the STIX format:
       *
       * id: {Event stream id} -> Like 1620249512318-0
       * event: {Event type} -> create / update / delete
       * data: { -> The complete event data
       *    markings: [] -> Array of markings IDS of the element
       *    origin: {Data Origin} -> Complex object with different information about the origin of the event
       *    data: {STIX data} -> The STIX representation of the data.
       *    message -> A simple string to easy understand the event
       *    version -> The version number of the event
       * }
       */
      override def onEvent(eventSource:EventSource, eventId:String, eventType:String, eventData:String):Unit = {
        callback.eventArrived(SseEvent(eventId, eventType, eventData))
      }

      override def onClosed(eventSource:EventSource) {
      }

      override def onFailure(eventSource:EventSource, t:Throwable, response:Response) {
        /* Restart the receiver in case of an error */
        restart(t)
      }

    }

    factory.newEventSource(request, listener)

  }

  def restart(t:Throwable): Unit = {

    val now = new java.util.Date().toString
    println(s"[CTIConnector] $now - Restart due to: ${t.getLocalizedMessage}")

    start()

  }
}
