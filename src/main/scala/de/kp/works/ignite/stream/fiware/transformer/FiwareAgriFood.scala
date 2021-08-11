package de.kp.works.ignite.stream.fiware.transformer
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

import de.kp.works.ignite.client.IgnitePut
import de.kp.works.ignite.stream.fiware.{FiwareNotification, FiwareTransformer}

object FiwareAgriFood extends FiwareTransformer {

  override def transformNotification(notification: FiwareNotification): (Seq[IgnitePut], Seq[IgnitePut]) = {

    val service = notification.service
    val servicePath = notification.servicePath

    val payload = notification.payload
    /*
     * {
     *  "data": [
     *      {
     *          "id": "Room1",
     *          "temperature": {
     *              "metadata": {},
     *              "type": "Float",
     *              "value": 28.5
     *          },
     *          "type": "Room"
     *      }
     *  ],
     *  "subscriptionId": "57458eb60962ef754e7c0998"
     * }
     */
    val data = payload.get("data").getAsJsonArray

    // TODO

    null

  }
}