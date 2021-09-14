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

import de.kp.works.ignite.stream.{IgniteStream, IgniteStreamContext}
import org.apache.ignite.binary.BinaryObject

import scala.actors.threadpool.Executors

class IgniteCTIContext(
  val stream:IgniteStream,
  val streamer:CTIStreamer[String,BinaryObject],
  numThreads:Int = 1) extends IgniteStreamContext {

  private val executorService = Executors.newFixedThreadPool(numThreads)

  def start():Unit = {
    try {

      /* Start OpenCTI streamer
       *
       * This streamer writes OpenCTI events
       * to an intermediate cache.
       */
      streamer.start()

      /* Start stream processor
       *
       * This stream processor starts from the intermediate
       * cache and transforms the respective entries into
       * their final format and destination
       */
      stream.processor.start()

      /* Initiate stream execution */
      executorService.execute(stream)

    } catch {
      case e:Exception => executorService.shutdown()
    }
  }

  /**
   * This method shuts down an ExecutorService in two phases,
   * first by calling 'shutdown' to reject incoming tasks, and
   * then calling 'shutdownNow'
   */

  def stop():Unit = {

    /* Stop OpenCTI streamer */
    streamer.stop()

    /* Shutdown processor */
    stream.processor.shutdown()

    /* Stop stream processing */
    executorService.shutdown()
    executorService.shutdownNow()

  }
}

