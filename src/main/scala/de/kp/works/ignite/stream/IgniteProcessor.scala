package de.kp.works.ignite.stream

import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.binary.BinaryObject

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

class IgniteProcessor(
  cache:IgniteCache[String,BinaryObject],
  ignite:Ignite) {

  /*
   * These flags control shutdown mechanism
   */
  protected var stopped:Boolean = true
  protected var writing:Boolean = false

  /**
   * This method must be called once before 'execute'
   * is called; otherwise streaming does not start
   */
  def start():Unit = {
    stopped = false
  }

  def shutdown():Unit = {
    /*
     * Make sure that there is no further stream
     * processing (see 'execute')
     */
    stopped = true
    /*
     * Wait until writing cycle is finished
     * to stop the associated thread
     */
    while (writing) {}

  }

  def write():Unit = {
    /*
     * This flag prevents write processing after
     * shutdown is initiated
     */
    if (stopped) return

    /* Flag indicates that writing is started */
    writing = true

    // TODO:: process events

    /* Flag indicates that writing cycle is finished */
    writing = false


  }
}
