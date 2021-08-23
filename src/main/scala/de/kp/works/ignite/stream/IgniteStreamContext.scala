package de.kp.works.ignite.stream
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

import org.apache.ignite.binary.BinaryObject
import scala.actors.threadpool._

abstract class IgniteStreamContext {

  def start():Unit
  def stop():Unit

}

class IgniteFiwareContext(
   val stream:IgniteStream,
   val streamer:fiware.FiwareStreamer[String,BinaryObject],
   numThreads:Int = 1) extends IgniteStreamContext {

  private val executorService = Executors.newFixedThreadPool(numThreads)

  def start():Unit = {
    try {

      /* Start Fiware streamer
       *
       * This streamer writes Fiware Broker notifications
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

    /* Stop Fiware streamer */
    streamer.stop()

    /* Shutdown processor */
    stream.processor.shutdown()

    /* Stop stream processing */
    executorService.shutdown()
    executorService.shutdownNow()

  }
}

class IgniteCTIContext(
   val stream:IgniteStream,
   val streamer:opencti.CTIStreamer[String,BinaryObject],
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


