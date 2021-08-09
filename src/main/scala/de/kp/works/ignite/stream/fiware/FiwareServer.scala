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

/**
 * The notification endpoint, the Orion Context Broker
 * sends (subscribed) notifications to.
 */
class FiwareServer {

  private var callback:Option[FiwareNotificationCallback] = None

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
}
