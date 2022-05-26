package de.kp.works.ignite.streamer.beat.table

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.ignite.sse.SseEvent
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * The [BeatTransformer] is implemented to transform
 * Beat SSE events of different event types.
 */
object BeatTransformer {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def transform(sseEvents: Seq[SseEvent]): Seq[(String, StructType, Seq[Row])] = ???

}
