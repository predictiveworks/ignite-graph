package de.kp.works.ignite.stream.fiware

import de.kp.works.ignite.client.IgnitePut

import scala.collection.mutable

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

trait FiwareTransformer {

  def transformNotification(notification:FiwareNotification):(Seq[IgnitePut], Seq[IgnitePut])

  def transform(notifications:Seq[FiwareNotification]):(Seq[IgnitePut], Seq[IgnitePut]) = {

    var edges = Seq.empty[IgnitePut]
    var vertices = Seq.empty[IgnitePut]

    notifications.foreach(notification => {

      val (v, e) = transformNotification(notification)

      vertices = vertices ++ v
      edges = edges ++ e

    })

    (vertices, edges)

  }

}
