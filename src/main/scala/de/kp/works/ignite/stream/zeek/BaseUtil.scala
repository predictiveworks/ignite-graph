package de.kp.works.ignite.stream.zeek
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import com.google.gson.{JsonArray, JsonObject}
import scala.collection.JavaConversions._

trait BaseUtil {

  protected def replaceCaptureLoss(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     * as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "ts_delta")

    newObject

  }

  def replaceConnection(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceConnId(newObject)
    val oldNames = List(
      "orig_bytes",
      "resp_bytes",
      "local_orig",
      "local_resp",
      "orig_pkts",
      "orig_ip_bytes",
      "resp_pkts",
      "resp_ip_bytes",
      "orig_l2_addr",
      "resp_l2_addr")

    oldNames.foreach(oldName =>
      newObject = replaceName(newObject, ZeekMapper.mapping(oldName), oldName))

    newObject

  }

  def replaceDceRpc(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)
    newObject
  }

  def replaceDhcp(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "lease_time")

    newObject = replaceInterval(newObject, "duration")
    newObject

  }

  def replaceDnp3(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  def replaceDns(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "dns_aa", "AA")
    newObject = replaceName(newObject, "dns_tc", "TC")
    newObject = replaceName(newObject, "dns_rd", "RD")
    newObject = replaceName(newObject, "dns_ra", "RA")
    newObject = replaceName(newObject, "dns_z", "Z")

    /*
     * Rename and transform 'TTLs'
     */
    newObject = replaceName(newObject, "dns_ttls", "TTLs")
    val ttls = newObject.remove("dns_ttls").getAsJsonArray

    val new_ttls = new JsonArray()
    ttls.foreach(ttl => {

      var interval = 0L
      try {

        val ts = ttl.getAsDouble
        interval = (ts * 1000).asInstanceOf[Number].longValue

      } catch {
        case _: Throwable => /* Do nothing */
      }

      new_ttls.add(interval)

    })

    newObject.add("dns_ttls", new_ttls)
    newObject

  }

  def replaceDpd(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject

  }

  def replaceFiles(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceName(newObject, "source_ips", "tx_hosts")
    newObject = replaceName(newObject, "destination_ips", "rx_hosts")

    newObject

  }

  /**
   * HINT: The provided connection parameters can be used
   * to build a unique (hash) connection identifier to join
   * with other data source like Osquery.
   */
  protected def replaceConnId(oldObject: JsonObject): JsonObject = {

    var newObject = oldObject
    val oldNames = List(
      "id.orig_h",
      "id.orig_p",
      "id.resp_h",
      "id.resp_p")

    oldNames.foreach(oldName =>
      newObject = replaceName(newObject, ZeekMapper.mapping(oldName), oldName))

    newObject

  }

  /** HELPER METHOD **/

  /**
   * Zeek specifies intervals (relative time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  protected def replaceInterval(jsonObject: JsonObject, intervalName: String): JsonObject = {

    if (jsonObject == null || jsonObject.get(intervalName) == null) return jsonObject

    var interval: Long = 0L
    try {

      val ts = jsonObject.get(intervalName).getAsDouble
      interval = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _: Throwable => /* Do nothing */
    }

    jsonObject.remove(intervalName)
    jsonObject.addProperty(intervalName, interval)

    jsonObject

  }

  protected def replaceName(jsonObject: JsonObject, newName: String, oldName: String): JsonObject = {

    try {

      if (jsonObject == null || jsonObject.get(oldName) == null) return jsonObject
      val value = jsonObject.remove(oldName)

      jsonObject.add(newName, value)
      jsonObject

    } catch {
      case _: Throwable => jsonObject
    }

  }

  /**
   * Zeek specifies timestamps (absolute time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  protected def replaceTime(jsonObject: JsonObject, timeName: String): JsonObject = {

    if (jsonObject == null || jsonObject.get(timeName) == null) return jsonObject

    var timestamp: Long = 0L
    try {

      val ts = jsonObject.get(timeName).getAsDouble
      timestamp = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _: Throwable => /* Do nothing */
    }

    jsonObject.remove(timeName)
    jsonObject.addProperty(timeName, timestamp)

    jsonObject

  }

}
