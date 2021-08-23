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

import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.IgniteProcessor
import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject

class CTIProcessor(
  cache:IgniteCache[String,BinaryObject],
  connect:IgniteConnect) extends IgniteProcessor(cache) {
  /**
   * The frequency we flush the internal store and write
   * data to the predefined output is currently set to
   * 2 times of the stream buffer flush frequency
   */
  override protected val flushWindow: Int = _

  /**
   * A helper method to apply the event query to the selected
   * Ignite cache, retrieve the results and write them to the
   * eventStore
   */
  override protected def extractEntries(): Unit = ???

  /**
   * A helper method to process the extracted cache entries
   * and transform and write to predefined output
   */
  override protected def processEntries(): Unit = ???
}
