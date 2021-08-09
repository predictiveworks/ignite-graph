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

import org.apache.ignite.{IgniteException, IgniteLogger}
import org.apache.ignite.stream.StreamAdapter

class FiwareStreamer[K,V](properties:Map[String,String]) extends StreamAdapter[FiwareMessage, K, V] {streamer =>

  /** Logger. */
  private val log:IgniteLogger = null

  /** State keeping. */
  private val stopped = true

  /** Start streamer  **/

  def start():Unit = {

    if (!stopped)
      throw new IgniteException("Attempted to start an already started Orion Streamer.")

  }

  /** Stop streamer **/

  def stop():Unit = {

    if (stopped)
      throw new IgniteException("Failed to stop Orion Streamer (already stopped).")

  }

}
