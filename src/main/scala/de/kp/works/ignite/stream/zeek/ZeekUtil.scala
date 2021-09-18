package de.kp.works.ignite.stream.zeek
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

import com.google.gson.{JsonElement, JsonObject}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConversions.iterableAsScalaIterable

object ZeekUtil {
  /**
   * capture_loss (&log)
   *
   * This logs evidence regarding the degree to which the packet capture process suffers
   * from measurement loss. The loss could be due to overload on the host or NIC performing
   * the packet capture or it could even be beyond the host. If you are capturing from a
   * switch with a SPAN port, it’s very possible that the switch itself could be overloaded
   * and dropping packets.
   *
   * Reported loss is computed in terms of the number of “gap events” (ACKs for a sequence
   * number that’s above a gap).
   *
   * {
   * 	"ts":1568132368.465338,
   * 	"ts_delta":32.282249,
   * 	"peer":"bro",
   * 	"gaps":0,
   * 	"acks":206,
   * 	"percent_lost":0.0
   * }
   */
  def fromCaptureLoss(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromCaptureLoss(log.getAsJsonObject, schema)
    })
  }

  def fromCaptureLoss(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     * as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "ts_delta")

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def capture_loss():StructType = {

    val fields = Array(

      /* ts: Timestamp for when the measurement occurred. The original
       * data type is a double and refers to seconds
       */
      StructField("ts", LongType, nullable = false),

      /* ts_delta: The time delay between this measurement and the last.
       * The original data type is a double and refers to seconds
       */
      StructField("ts_delta", LongType, nullable = false),

      /* peer: In the event that there are multiple Zeek instances logging
       * to the same host, this distinguishes each peer with its individual
       * name.
       */
      StructField("peer", StringType, nullable = false),

      /* count: Number of missed ACKs from the previous measurement interval.
       */
      StructField("count", IntegerType, nullable = false),

      /* acks: Total number of ACKs seen in the previous measurement interval.
       */
      StructField("acks", IntegerType, nullable = false),

      /* percent_lost: Percentage of ACKs seen where the data being ACKed wasn’t seen.
       */
      StructField("percent_lost", DoubleType, nullable = false)

    )

    StructType(fields)

  }
  /**
   *
   * connection (&log)
   *
   * This logs the tracking/logging of general information regarding TCP, UDP, and ICMP traffic.
   * For UDP and ICMP, “connections” are to be interpreted using flow semantics (sequence of
   * packets from a source host/port to a destination host/port). Further, ICMP “ports” are to
   * be interpreted as the source port meaning the ICMP message type and the destination port
   * being the ICMP message code.
   *
   * {
   * 	"ts":1547188415.857497,
   * 	"uid":"CAcJw21BbVedgFnYH3",
   * 	"id.orig_h":"192.168.86.167",
   * 	"id.orig_p":38339,
   * 	"id.resp_h":"192.168.86.1",
   * 	"id.resp_p":53,
   * 	"proto":"udp",
   * 	"service":"dns",
   * 	"duration":0.076967,
   * 	"orig_bytes":75,
   * 	"resp_bytes":178,
   * 	"conn_state":"SF",
   * 	"local_orig":true,
   * 	"local_resp":true,
   * 	"missed_bytes":0,
   * 	"history":"Dd",
   * 	"orig_pkts":1,
   * 	"orig_ip_bytes":103,
   * 	"resp_pkts":1,
   * 	"resp_ip_bytes":206,
   * 	"tunnel_parents":[]
   * }
   */
  def fromConnection(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromConnection(log.getAsJsonObject, schema)
    })
  }

  def fromConnection(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and transform time values;
     as these fields are declared to be non nullable, we do not check
     * their existence
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "source_bytes", "orig_bytes")
    newObject = replaceName(newObject, "destination_bytes", "resp_bytes")

    newObject = replaceName(newObject, "source_local", "local_orig")
    newObject = replaceName(newObject, "destination_local", "local_resp")

    newObject = replaceName(newObject, "source_pkts", "orig_pkts")
    newObject = replaceName(newObject, "source_ip_bytes", "orig_ip_bytes")

    newObject = replaceName(newObject, "destination_pkts", "resp_pkts")
    newObject = replaceName(newObject, "destination_ip_bytes", "resp_ip_bytes")

    newObject = replaceName(newObject, "source_l2_addr", "orig_l2_addr")
    newObject = replaceName(newObject, "destination_l2_addr", "resp_l2_addr")

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def connection():StructType = {

    var fields = Array(

      /* ts: Timestamp for when the measurement occurred. The original
       * data type is a double and refers to seconds
       */
      StructField("ts", LongType, nullable = false),

      /* uid: A unique identifier of the connection.
       */
      StructField("uid", StringType, nullable = false)

    )

    /* id
     */
    fields = fields ++ conn_id()

    fields = fields ++ Array(
      /* proto: A connection’s transport-layer protocol. Supported values are
       * unknown_transport, tcp, udp, icmp.
       */
      StructField("proto", StringType, nullable = false),

      /* service: An identification of an application protocol being sent over
       * the connection.
       */
      StructField("service", StringType, nullable = true),

      /* duration: A temporal type representing a relative time. How long the connection
       * lasted. For 3-way or 4-way connection tear-downs, this will not include the final
       * ACK. The original data type is a double and refers to seconds
       */
      StructField("duration", LongType, nullable = true),

      /* orig_bytes: The number of payload bytes the originator sent. For TCP this is taken
       * from sequence numbers and might be inaccurate (e.g., due to large connections).
       */
      StructField("source_bytes", IntegerType, nullable = true),

      /* resp_bytes: The number of payload bytes the responder sent. See orig_bytes.
       */
      StructField("destination_bytes", IntegerType, nullable = true),

      /* conn_state: Possible conn_state values.
       */
      StructField("conn_state", StringType, nullable = true),

      /* local_orig: If the connection is originated locally, this value will be T.
       * If it was originated remotely it will be F.
       */
      StructField("source_local", BooleanType, nullable = true),

      /* local_resp: If the connection is responded to locally, this value will be T. If it was
       * responded to remotely it will be F.
       */
      StructField("destination_local", BooleanType, nullable = true),

      /* missed_bytes: Indicates the number of bytes missed in content gaps, which is representative
       * of packet loss. A value other than zero will normally cause protocol analysis to fail but
       * some analysis may have been completed prior to the packet loss.
       */
      StructField("missed_bytes", IntegerType, nullable = true),

      /* history: Records the state history of connections as a string of letters.
       */
      StructField("history", StringType, nullable = true),

      /* orig_pkts: Number of packets that the originator sent.
       */
      StructField("source_pkts", IntegerType, nullable = true),

      /* orig_ip_bytes: Number of IP level bytes that the originator sent (as seen on the wire,
       * taken from the IP total_length header field).
       */
      StructField("source_ip_bytes", IntegerType, nullable = true),

      /* resp_pkts: Number of packets that the responder sent.
       */
      StructField("destination_pkts", IntegerType, nullable = true),

      /* resp_ip_bytes: Number of IP level bytes that the responder sent (as seen on the wire,
       * taken from the IP total_length header field).
       */
      StructField("destination_ip_bytes", IntegerType, nullable = true),

      /* tunnel_parents: If this connection was over a tunnel, indicate the uid values for any
       * encapsulating parent connections used over the lifetime of this inner connection.
       */
      StructField("tunnel_parents", ArrayType(StringType), nullable = true),

      /* orig_l2_addr: Link-layer address of the originator, if available.
       */
      StructField("source_l2_addr", StringType, nullable = true),

      /* resp_l2_addr: Link-layer address of the responder, if available.
       */
      StructField("destination_l2_addr",StringType, nullable = true),

      /* vlan: The outer VLAN for this connection, if applicable.
       */
      StructField("vlan", IntegerType, nullable = true),

      /* inner_vlan: The inner VLAN for this connection, if applicable.
       */
      StructField("inner_vlan", IntegerType, nullable = true),

      /* speculative_service: Protocol that was determined by a matching signature after the beginning
       * of a connection. In this situation no analyzer can be attached and hence the data cannot be
       * analyzed nor the protocol can be confirmed.
       */
      StructField("speculative_service", StringType, nullable = true)

    )

    StructType(fields)

  }
  /**
   * dce_rpc (&log)
   *
   * {
   * 	"ts":1361916332.298338,
   * 	"uid":"CsNHVHa1lzFtvJzT8",
   * 	"id.orig_h":"172.16.133.6",
   * 	"id.orig_p":1728,
   * 	"id.resp_h":"172.16.128.202",
   * 	"id.resp_p":445,"rtt":0.09211,
   * 	"named_pipe":"\u005cPIPE\u005cbrowser",
   * 	"endpoint":"browser",
   * 	"operation":"BrowserrQueryOtherDomains"
   * }
   */
  def fromDceRpc(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromDceRpc(log.getAsJsonObject, schema)
    })
  }

  def fromDceRpc(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def dce_rpc():StructType = {

    var fields = Array(

      /* ts: Timestamp for when the event happened.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: A unique identifier of the connection.
       */
      StructField("uid", StringType, nullable = false)

    )

    /* id
     */
    fields = fields ++ conn_id()

    fields = fields ++ Array(
      /* rtt: Round trip time from the request to the response. If either the
       * request or response was not seen, this will be null.
       */
      StructField("rtt", LongType, nullable = true),

      /* named_pipe: Remote pipe name.
       */
      StructField("named_pipe", StringType, nullable = true),

      /* endpoint: Endpoint name looked up from the uuid.
       */
      StructField("endpoint", StringType, nullable = true),

      /* operation: Operation seen in the call.
       */
      StructField("operation", StringType, nullable = true)

    )

    StructType(fields)

  }

  private def json2Row(jsonObject:JsonObject, schema:StructType):Row = {

    val values = schema.fields.map(field => {

      val fieldName = field.name
      val fieldType = field.dataType

      fieldType match {
        case ArrayType(StringType, true) =>
          getStringArray(jsonObject, fieldName, field.nullable)

        case ArrayType(StringType, false) =>
          getStringArray(jsonObject, fieldName, field.nullable)

        case BooleanType =>
          getBoolean(jsonObject, fieldName, field.nullable)

        case DoubleType =>
          getDouble(jsonObject, fieldName, field.nullable)

        case IntegerType =>
          getInt(jsonObject, fieldName, field.nullable)

        case LongType =>
          getLong(jsonObject, fieldName, field.nullable)

        case StringType =>
          getString(jsonObject, fieldName, field.nullable)

        case _ => throw new Exception(s"Data type `$fieldType.toString` is not supported.")
      }

    }).toSeq

    Row.fromSeq(values)

  }

  private def getBoolean(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Boolean = {

    try {
      jsonObject.get(fieldName).getAsBoolean

    } catch {
      case _:Throwable =>
        if (nullable) false
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  private def getDouble(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Double = {

    try {
      jsonObject.get(fieldName).getAsDouble

    } catch {
      case _:Throwable =>
        if (nullable) Double.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  private def getInt(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Int = {

    try {
      jsonObject.get(fieldName).getAsInt

    } catch {
      case _:Throwable =>
        if (nullable) Int.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  private def getLong(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Long = {

    try {
      jsonObject.get(fieldName).getAsLong

    } catch {
      case _:Throwable =>
        if (nullable) Long.MaxValue
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  private def getString(jsonObject:JsonObject, fieldName:String, nullable:Boolean):String = {

    try {
      jsonObject.get(fieldName).getAsString

    } catch {
      case _:Throwable =>
        if (nullable) ""
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }

  private def getStringArray(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Array[String] = {

    try {
      jsonObject.get(fieldName).getAsJsonArray
        .map(json => json.getAsString)
        .toArray

    } catch {
      case _:Throwable =>
        if (nullable) Array.empty[String]
        else
          throw new Exception(s"No value provided for field `$fieldName`.")
    }

  }
  /********************
   *
   * BASE SCHEMAS
   *
   *******************/

  private def certificate(): Array[StructField] = {

    val fields = Array(

      /* certificate.version: Version number.
       */
      StructField("cert_version", IntegerType, nullable = false),

      /* certificate.serial: Serial number.
       */
      StructField("cert_serial", StringType, nullable = false),

      /* certificate.subject: Subject.
       */
      StructField("cert_subject",  StringType, nullable = false),

      /* certificate.cn: Last (most specific) common name.
       */
      StructField("cert_cn",  StringType, nullable = true),

      /* certificate.not_valid_before: Timestamp before when certificate is not valid.
       */
      StructField("cert_not_valid_before", LongType, nullable = false),

      /* certificate.not_valid_after: Timestamp after when certificate is not valid.
       */
      StructField("cert_not_valid_after", LongType, nullable = false),

      /* certificate.key_alg: Name of the key algorithm.
       */
      StructField("cert_key_alg",  StringType, nullable = false),

      /* certificate.sig_alg: Name of the signature algorithm.
       */
      StructField("cert_sig_alg",  StringType, nullable = false),

      /* certificate.key_type: Key type, if key parsable by openssl (either rsa, dsa or ec).
       */
      StructField("cert_key_type", StringType, nullable = true),

      /* certificate.key_length: Key length in bits.
       */
      StructField("cert_key_length", IntegerType, nullable = true),

      /* certificate.exponent: Exponent, if RSA-certificate.
       */
      StructField("cert_exponent", StringType, nullable = true),

      /* certificate.curve: Curve, if EC-certificate.
       */
      StructField("cert_curve", StringType, nullable = true)

    )

    fields

  }

  private def conn_id():Array[StructField] = {

    val fields = Array(

      /* id.orig_h: The originator’s IP address.
       */
      StructField("source_ip", StringType, nullable = false),

      /* id.orig_p: The originator’s port number.
       */
      StructField("source_port", IntegerType, nullable = false),

      /* id.resp_h: The responder’s IP address.
       */
      StructField("destination_ip", StringType, nullable = false),

      /* id.resp_p: The responder’s port number.
       */
      StructField("destination_port", IntegerType, nullable = false)

    )

    fields

  }

  private def conn_id_nullable:Array[StructField] = {

    val fields = Array(

      /* id.orig_h: The originator’s IP address.
       */
      StructField("source_ip", StringType, nullable = true),

      /* id.orig_p: The originator’s port number.
       */
      StructField("source_port", IntegerType, nullable = true),

      /* id.resp_h: The responder’s IP address.
       */
      StructField("destination_ip", StringType, nullable = true),

      /* id.resp_p: The responder’s port number.
       */
      StructField("destination_port", IntegerType, nullable = true)

    )

    fields

  }

  private def replaceCertificate(oldObject:JsonObject):JsonObject = {

    var newObject = oldObject

    newObject = replaceTime(newObject, "certificate.not_valid_before")
    newObject = replaceTime(newObject, "certificate.not_valid_after")

    newObject = replaceName(newObject, "cert_version", "certificate.version")
    newObject = replaceName(newObject, "cert_serial", "certificate.serial")

    newObject = replaceName(newObject, "cert_subject", "certificate.subject")
    newObject = replaceName(newObject, "cert_cn", "certificate.cn")

    newObject = replaceName(newObject, "cert_not_valid_before", "certificate.not_valid_before")
    newObject = replaceName(newObject, "cert_not_valid_after", "certificate.not_valid_after")

    newObject = replaceName(newObject, "cert_key_alg", "certificate.key_alg")
    newObject = replaceName(newObject, "cert_sig_alg", "certificate.sig_alg")

    newObject = replaceName(newObject, "cert_key_type", "certificate.key_type")
    newObject = replaceName(newObject, "cert_key_length", "certificate.key_length")

    newObject = replaceName(newObject, "cert_exponent", "certificate.exponent")
    newObject = replaceName(newObject, "cert_curve", "certificate.curve")

    newObject

  }

  private def replaceName(jsonObject:JsonObject, newName:String, oldName:String):JsonObject = {

    try {

      if (jsonObject == null || jsonObject.get(oldName) == null) return jsonObject
      val value = jsonObject.remove(oldName)

      jsonObject.add(newName, value)
      jsonObject

    } catch {
      case _:Throwable => jsonObject
    }

  }
  /*
   * Zeek specifies timestamps (absolute time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  private def replaceTime(jsonObject:JsonObject, timeName:String):JsonObject = {

    if (jsonObject == null || jsonObject.get(timeName) == null) return jsonObject

    var timestamp:Long = 0L
    try {

      val ts = jsonObject.get(timeName).getAsDouble
      timestamp = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _:Throwable => /* Do nothing */
    }

    jsonObject.remove(timeName)
    jsonObject.addProperty(timeName, timestamp)

    jsonObject

  }
  /*
   * Zeek specifies intervals (relative time) as Double
   * that defines seconds; this method transforms them
   * into milliseconds
   */
  private def replaceInterval(jsonObject:JsonObject, intervalName:String):JsonObject = {

    if (jsonObject == null || jsonObject.get(intervalName) == null) return jsonObject

    var interval:Long = 0L
    try {

      val ts = jsonObject.get(intervalName).getAsDouble
      interval = (ts * 1000).asInstanceOf[Number].longValue()

    } catch {
      case _:Throwable => /* Do nothing */
    }

    jsonObject.remove(intervalName)
    jsonObject.addProperty(intervalName, interval)

    jsonObject

  }

  private def replaceConnId(oldObject:JsonObject):JsonObject = {

    var newObject = oldObject

    newObject = replaceName(newObject, "source_ip", "id.orig_h")
    newObject = replaceName(newObject, "source_port", "id.orig_p")

    newObject = replaceName(newObject, "destination_ip", "id.resp_h")
    newObject = replaceName(newObject, "destination_port", "id.resp_p")

    newObject

  }

}
