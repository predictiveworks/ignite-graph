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
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.{IgniteCTIContext, IgniteStream, IgniteStreamContext}
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject

/**
 * [CTIIgnite] is responsible for streaming threat intelligence
 * events into a temporary cache and also their final processing
 * as edges & vertices of an information network.
 */
class CTIIgnite(connect:IgniteConnect) {

  if (!CommonConfig.isInit)
    throw new Exception("[CTIIgnite] No configuration initialized. Streaming cannot be started.")

  private val ignite = connect.getIgnite
  private val conf = CommonConfig.getCTIStreamerCfg

  def buildStream:Option[IgniteStreamContext] = {

    try {

      val (cache,streamer) = prepareCTIStreamer
      val numThreads = conf.getInt("numThreads")

      val stream: IgniteStream = new IgniteStream {
        override val processor = new CTIProcessor(cache, connect)
      }

      Some(new IgniteCTIContext(stream,streamer, numThreads))

    } catch {
      case t: Throwable =>
        println(s"[ERROR] Stream preparation for 'ingestion' operation failed: ${t.getLocalizedMessage}")
        None
    }
  }

  private def prepareCTIStreamer:(IgniteCache[String,BinaryObject],CTIStreamer[String,BinaryObject]) = {

    // TODO
    null
  }

}
