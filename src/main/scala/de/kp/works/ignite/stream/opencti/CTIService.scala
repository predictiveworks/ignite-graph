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

import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.ssl.SslOptions

class CTIService {

  private var callback:Option[CTIEventCallback] = None
  /**
   * Specify the callback to be used by this service
   * to send OpenCTI events to the respective Ignite
   * cache.
   *
   * The current implementation leverages the CTI
   * Streamer as callback
   */
  def setCallback(callback:CTIEventCallback):CTIService = {
    this.callback = Some(callback)
    this
  }
  /**
   * This method launches the OpenCTI connection and
   * listens to published SSE
   */
  def start():Unit = {

    if (callback.isEmpty)
      throw new Exception("[CTIService] No callback specified to send events to.")
    /*
     * After having started the Http(s) server,
     * the server is started that connects to
     * OpenCTI server and retrieves SSE
     *
     * OpenCTI streams (server) --> CTIReceiver
     *
     * The receiver is an SSE client that listens
     * to published threat intelligence events.
     */
    val receiverCfg = CommonConfig.getCTIReceiverCfg
    val endpoint = receiverCfg.getString("endpoint")

    val authToken = {
      val value = receiverCfg.getString("authToken")
      if (value.isEmpty) None else Some(value)
    }
    /*
     * Transport security configuration used to
     * establish a Http(s) connection to the server.
     */
    val securityCfg = receiverCfg.getConfig("security")
    val sslOptions = SslOptions.getOptions(securityCfg)

    val numThreads = receiverCfg.getInt("numThreads")
    val receiver = new CTIReceiver(
      endpoint,
      callback.get,
      authToken,
      Some(sslOptions),
      numThreads
    )

    receiver.start()

  }

  def stop():Unit = {

  }

}