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

import com.google.gson.{JsonArray, JsonElement, JsonObject}
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

  /**
   * dhcp (&log)
   *
   * {
   * 	"ts":1476605498.771847,
   * 	"uids":["CmWOt6VWaNGqXYcH6","CLObLo4YHn0u23Tp8a"],
   * 	"client_addr":"192.168.199.132",
   * 	"server_addr":"192.168.199.254",
   * 	"mac":"00:0c:29:03:df:ad",
   * 	"host_name":"DESKTOP-2AEFM7G",
   * 	"client_fqdn":"DESKTOP-2AEFM7G",
   * 	"domain":"localdomain",
   * 	"requested_addr":"192.168.199.132",
   * 	"assigned_addr":"192.168.199.132",
   * 	"lease_time":1800.0,
   * 	"msg_types":["REQUEST","ACK"],
   * 	"duration":0.000161
   * }
   */
  def fromDhcp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromDhcp(log.getAsJsonObject, schema)
    })
  }

  def fromDhcp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "lease_time")

    newObject = replaceInterval(newObject, "duration")

    /* Transform into row */
    json2Row(newObject, schema)


  }

  def dhcp():StructType = {

    val fields = Array(

      /* ts: The earliest time at which a DHCP message over the associated
       * connection is observed.
       */
      StructField("ts", LongType, nullable = false),

      /* uids: A series of unique identifiers of the connections over which
       * DHCP is occurring. This behavior with multiple connections is unique
       * to DHCP because of the way it uses broadcast packets on local networks.
       */
      StructField("uids", ArrayType(StringType), nullable = false),

      /* client_addr: IP address of the client. If a transaction is only a client
       * sending INFORM messages then there is no lease information exchanged so
       * this is helpful to know who sent the messages.
       *
       * Getting an address in this field does require that the client sources at
       * least one DHCP message using a non-broadcast address.
       */
      StructField("client_addr", StringType, nullable = true),

      /* server_addr: IP address of the server involved in actually handing out the
       * lease. There could be other servers replying with OFFER messages which won’t
       * be represented here. Getting an address in this field also requires that the
       * server handing out the lease also sources packets from a non-broadcast IP address.
       */
      StructField("server_addr", StringType, nullable = true),

      /* mac: Client’s hardware address.
       */
      StructField("mac",StringType, nullable = true),

      /* host_name: Name given by client in Hostname.
       */
      StructField("host_name", StringType, nullable = true),

      /* client_fqdn: FQDN given by client in Client FQDN
       */
      StructField("client_fqdn", StringType, nullable = true),

      /* domain: Domain given by the server
       */
      StructField("domain",StringType, nullable = true),

      /* requested_addr: IP address requested by the client.
       */
      StructField("requested_addr",StringType, nullable = true),

      /* assigned_addr: IP address assigned by the server.
       */
      StructField("assigned_addr",StringType, nullable = true),

      /* lease_time: IP address lease interval.
       */
      StructField("lease_time", LongType, nullable = true),

      /* client_message: Message typically accompanied with a DHCP_DECLINE so the
       * client can tell the server why it rejected an address.
       */
      StructField("client_message", StringType, nullable = true),

      /* server_message: Message typically accompanied with a DHCP_NAK to let the
       * client know why it rejected the request.
       */
      StructField("server_message", StringType, nullable = true),

      /* msg_types: The DHCP message types seen by this DHCP transaction
       */
      StructField("msg_types",ArrayType(StringType), nullable = true),

      /* duration: Duration of the DHCP “session” representing the time from the
       * first message to the last.
       */
      StructField("duration", LongType, nullable = true),

      /* msg_orig: The address that originated each message from the msg_types field.
       */
      StructField("msg_orig", ArrayType(StringType), nullable = true),

      /* client_software: Software reported by the client in the vendor_class option.
       */
      StructField("client_software", StringType, nullable = true),

      /* server_software: Software reported by the server in the vendor_class option.
       */
      StructField("server_software", StringType, nullable = true),

      /* circuit_id: Added by DHCP relay agents which terminate switched or permanent circuits.
       * It encodes an agent-local identifier of the circuit from which a DHCP client-to-server
       * packet was received. Typically it should represent a router or switch interface number.
       */
      StructField("circuit_id", StringType, nullable = true),

      /* agent_remote_id: A globally unique identifier added by relay agents to identify the
       * remote host end of the circuit.
       */
      StructField("agent_remote_id", StringType, nullable = true),

      /* subscriber_id: The subscriber ID is a value independent of the physical network configuration
       * so that a customer’s DHCP configuration can be given to them correctly no matter where they are
       * physically connected.
       */
      StructField("subscriber_id", StringType, nullable = true)

    )

    StructType(fields)

  }
  /**
   * dnp3 (&log)
   *
   * A Log of very basic DNP3 analysis script that just records
   * requests and replies.
   *
   * {
   * 	"ts":1227729908.705944,
   * 	"uid":"CQV6tj1w1t4WzQpHoe",
   * 	"id.orig_h":"127.0.0.1",
   * 	"id.orig_p":42942,
   * 	"id.resp_h":"127.0.0.1",
   * 	"id.resp_p":20000,
   * 	"fc_request":"READ"
   * }
   */
  def fromDnp3(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromDnp3(log.getAsJsonObject, schema)
    })
  }

  def fromDnp3(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def dnp3():StructType = {

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

      /* fc_request: The name of the function message in the request.
       */
      StructField("fc_request",StringType, nullable = true),

      /* fc_reply: The name of the function message in the reply.
       */
      StructField("fc_reply", StringType, nullable = true),

      /* iin: The response’s “internal indication number”.
       */
      StructField("iin", IntegerType, nullable = true)

    )

    StructType(fields)

  }
  /**
   * dns (&log)
   *
   * {
   * 	"ts":1547188415.857497,
   * 	"uid":"CAcJw21BbVedgFnYH3",
   * 	"id.orig_h":"192.168.86.167",
   * 	"id.orig_p":38339,
   * 	"id.resp_h":"192.168.86.1",
   * 	"id.resp_p":53,
   * 	"proto":"udp",
   * 	"trans_id":15209,
   * 	"rtt":0.076967,
   * 	"query":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
   * 	"qclass":1,
   * 	"qclass_name":"C_INTERNET",
   * 	"qtype":1,
   * 	"qtype_name":"A",
   * 	"rcode":0,
   * 	"rcode_name":"NOERROR",
   * 	"AA":false,
   * 	"TC":false,
   * 	"RD":true,
   * 	"RA":true,
   * 	"Z":0,
   * 	"answers":["proxy-production-us-west1.gcp.cloud.es.io","proxy-production-us-west1-v1-009.gcp.cloud.es.io","35.199.178.4"],
   * 	"TTLs":[119.0,119.0,59.0],
   * 	"rejected":false
   * }
   */
  def fromDns(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromDns(log.getAsJsonObject, schema)
    })
  }

  def fromDns(oldObject:JsonObject, schema:StructType):Row = {

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
        case _:Throwable => /* Do nothing */
      }

      new_ttls.add(interval)

    })

    newObject.add("dns_ttls", new_ttls)

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def dns():StructType = {

    var fields = Array(

      /* ts: The earliest time at which a DNS protocol message over the
       * associated connection is observed.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: A unique identifier of the connection over which DNS
       * messages are being transferred.
       */
      StructField("uid", StringType, nullable = false)

    )

    /* id
     */
    fields = fields ++ conn_id()

    fields = fields ++ Array(
      /* proto: The transport layer protocol of the connection.
        */
      StructField("proto", StringType, nullable = false),

      /* trans_id: A 16-bit identifier assigned by the program that generated
       * the DNS query. Also used in responses to match up replies to outstanding
       * queries.
       */
      StructField("trans_id", IntegerType, nullable = true),

      /* rtt: Round trip time for the query and response. This indicates the delay
       * between when the request was seen until the answer started.
       */
      StructField("rtt", LongType, nullable = true),

      /* query: The domain name that is the subject of the DNS query.
       */
      StructField("query", StringType, nullable = true),

      /* qclass: The QCLASS value specifying the class of the query.
       */
      StructField("qclass", IntegerType, nullable = true),

      /* qclass_name: A descriptive name for the class of the query.
       */
      StructField("qclass_name", StringType, nullable = true),

      /* qtype: A QTYPE value specifying the type of the query.
       */
      StructField("qtype", IntegerType, nullable = true),

      /* qtype_name: A descriptive name for the type of the query.
       */
      StructField("qtype_name", StringType, nullable = true),

      /* rcode: The response code value in DNS response messages.
       */
      StructField("rcode", IntegerType, nullable = true),

      /* rcode_name: A descriptive name for the response code value.
       */
      StructField("rcode_name",StringType, nullable = true),

      /* AA: The Authoritative Answer bit for response messages specifies
       * that the responding name server is an authority for the domain
       * name in the question section.
       */
      StructField("dns_aa", BooleanType, nullable = true),

      /* TC: The Recursion Desired bit in a request message indicates that
       * the client wants recursive service for this query.
       */
      StructField("dns_tc", BooleanType, nullable = true),

      /* RA: The Recursion Available bit in a response message indicates that
       * the name server supports recursive queries.
       */
      StructField("dns_ra", BooleanType, nullable = true),

      /* Z: A reserved field that is usually zero in queries and responses.
       */
      StructField("dns_z", BooleanType, nullable = true),

      /* answers: The set of resource descriptions in the query answer.
       */
      StructField("answers", ArrayType(StringType), nullable = true),

      /* TTLs: The caching intervals of the associated RRs described by the answers field.
       */
      StructField("dns_ttls", ArrayType(LongType), nullable = true),

      /* rejected: The DNS query was rejected by the server.
       */
      StructField("rejected",BooleanType, nullable = true),

      /* auth: Authoritative responses for the query.
       */
      StructField("auth", ArrayType(StringType), nullable = true),

      /* addl: Additional responses for the query.
       */
      StructField("addl", ArrayType(StringType), nullable = true)

    )

    StructType(fields)

  }

  /**
   * dpd (&log)
   *
   * Dynamic protocol detection failures.
   *
   * {
   * 	"ts":1507567500.423033,
   * 	"uid":"CRrT7S1ccw9H6hzCR",
   * 	"id.orig_h":"192.168.10.31",
   * 	"id.orig_p":49285,
   * 	"id.resp_h":"192.168.10.10",
   * 	"id.resp_p":445,
   * 	"proto":"tcp",
   * 	"analyzer":"DCE_RPC",
   * 	"failure_reason":"Binpac exception: binpac exception: \u0026enforce violation : DCE_RPC_Header:rpc_vers"
   * }
   */
  def fromDpd(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromDpd(log.getAsJsonObject, schema)
    })
  }

  def fromDpd(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def dpd():StructType = {

    var fields = Array(

      /* ts: timestamp for when protocol analysis failed.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: Connection unique ID.
       */
      StructField("uid", StringType, nullable = false)

    )

    /* id
     */
    fields = fields ++ conn_id()

    fields = fields ++ Array(

      /* proto: The transport layer protocol of the connection.
       */
      StructField("proto", StringType, nullable = false),

      /* analyzer: The analyzer that generated the violation.
       */
      StructField("analyzer", StringType, nullable = false),

      /* failure_reason: The textual reason for the analysis failure.
       */
      StructField("failure_reason", StringType, nullable = false),

      /* packet_segment: A chunk of the payload that most likely resulted in
       * the protocol violation.
       */
      StructField("packet_segment", StringType, nullable = true)

    )

    StructType(fields)

  }

  /**
   * files (&log)
   *
   * {
   * 	"ts":1547688796.636812,
   * 	"fuid":"FMkioa222mEuM2RuQ9",
   * 	"tx_hosts":["35.199.178.4"],
   * 	"rx_hosts":["10.178.98.102"],
   * 	"conn_uids":["C8I0zn3r9EPbfLgta6"],
   * 	"source":"SSL",
   * 	"depth":0,
   * 	"analyzers":["X509","MD5","SHA1"],
   * 	"mime_type":"application/pkix-cert",
   * 	"duration":0.0,
   * 	"local_orig":false,
   * 	"is_orig":false,
   * 	"seen_bytes":947,
   * 	"missing_bytes":0,
   * 	"overflow_bytes":0,
   * 	"timedout":false,
   * 	"md5":"79e4a9840d7d3a96d7c04fe2434c892e",
   * 	"sha1":"a8985d3a65e5e5c4b2d7d66d40c6dd2fb19c5436"
   * }
   */
  def fromFiles(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromFiles(log.getAsJsonObject, schema)
    })
  }

  def fromFiles(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "duration")

    newObject = replaceName(newObject, "source_ips", "tx_hosts")
    newObject = replaceName(newObject, "destination_ips", "rx_hosts")

    /* Transform into row */
    json2Row(newObject, schema)

  }
  /*
   * IMPORTANT: Despite the example above (from Filebeat),
   * the current Zeek documentation (v3.1.2) does not specify
   * a connection id
   */
  def files():StructType = {

    val fields = Array(

      /* ts: The time when the file was first seen.
       */
      StructField("ts", LongType, nullable = false),

      /* fuid: An identifier associated with a single file.
       */
      StructField("fuid", StringType, nullable = false),

      /* tx_hosts: If this file was transferred over a network connection
       * this should show the host or hosts that the data sourced from.
       */
      StructField("source_ips", ArrayType(StringType), nullable = true),

      /* rx_hosts: If this file was transferred over a network connection
       * this should show the host or hosts that the data traveled to.
       */
      StructField("destination_ips", ArrayType(StringType), nullable = true),

      /* conn_uids: Connection UIDs over which the file was transferred.
       */
      StructField("conn_uids", ArrayType(StringType), nullable = true),

      /* source: An identification of the source of the file data.
       * E.g. it may be a network protocol over which it was transferred,
       * or a local file path which was read, or some other input source.
       */
      StructField("source", StringType, nullable = true),

      /* depth: A value to represent the depth of this file in relation to
       * its source. In SMTP, it is the depth of the MIME attachment on the
       * message.
       *
       * In HTTP, it is the depth of the request within the TCP connection.
       */
      StructField("depth", IntegerType, nullable = true),

      /* analyzers: A set of analysis types done during the file analysis.
       */
      StructField("analyzers", ArrayType(StringType), nullable = true),

      /* mime_type: A mime type provided by the strongest file magic signature
       * match against the bof_buffer field of fa_file, or in the cases where
       * no buffering of the beginning of file occurs, an initial guess of the
       * mime type based on the first data seen.
       */
      StructField("mime_type", StringType, nullable = true),

      /* filename: A filename for the file if one is available from the source
       * for the file. These will frequently come from “Content-Disposition”
       * headers in network protocols.
       */
      StructField("filename", StringType, nullable = true),

      /* duration: The duration the file was analyzed for.
       */
      StructField("duration", LongType, nullable = true),

      /* local_orig: If the source of this file is a network connection, this field
       * indicates if the data originated from the local network or not as determined
       * by the configured
       */
      StructField("local_orig", BooleanType, nullable = true),

      /* is_orig: If the source of this file is a network connection, this field indicates
       * if the file is being sent by the originator of the connection or the responder.
       */
      StructField("is_orig", BooleanType, nullable = true),

      /* seen_bytes: Number of bytes provided to the file analysis engine for the file.
       */
      StructField("seen_bytes", IntegerType, nullable = true),

      /* total_bytes: Total number of bytes that are supposed to comprise the full file.
       */
      StructField("total_bytes", IntegerType, nullable = true),

      /* missing_bytes: The number of bytes in the file stream that were completely missed
       * during the process of analysis e.g. due to dropped packets.
       */
      StructField("missing_bytes", IntegerType, nullable = true),

      /* overflow_bytes: The number of bytes in the file stream that were not delivered
       * to stream file analyzers. This could be overlapping bytes or bytes that couldn’t
       * be reassembled.
       */
      StructField("overflow_bytes", IntegerType, nullable = true),

      /* timedout: Whether the file analysis timed out at least once for the file.
       */
      StructField("timedout", BooleanType, nullable = true),

      /* parent_fuid: Identifier associated with a container file from which this one
       * was extracted as part of the file analysis.
       */
      StructField("parent_fuid", StringType, nullable = true),

      /* md5: An MD5 digest of the file contents.
       */
      StructField("md5", StringType, nullable = true),

      /* sha1: A SHA1 digest of the file contents.
       */
      StructField("sha1", StringType, nullable = true),

      /* sha256: A SHA256 digest of the file contents.
       */
      StructField("sha256", StringType, nullable = true),

      /* extracted: Local filename of extracted file.
       */
      StructField("extracted", StringType, nullable = true),

      /* extracted_cutoff: Set to true if the file being extracted was cut off so the
       * whole file was not logged.
       */
      StructField("extracted_cutoff", BooleanType, nullable = true),

      /* extracted_size: The number of bytes extracted to disk.
       */
      StructField("extracted_size", IntegerType, nullable = true),

      /* entropy: The information density of the contents of the file, expressed
       * as a number of bits per character.
       */
      StructField("entropy", DoubleType, nullable = true)

    )

    StructType(fields)

  }
  /**
   * ftp (& log)
   *
   * FTP activity
   *
   * {
   * 	"ts":1187379104.955342,
   * 	"uid":"CpQoCn3o28tke89zv9",
   * 	"id.orig_h":"192.168.1.182",
   * 	"id.orig_p":62014,
   * 	"id.resp_h":"192.168.1.231",
   * 	"id.resp_p":21,
   * 	"user":"ftp",
   * 	"password":"ftp",
   * 	"command":"EPSV",
   * 	"reply_code":229,
   * 	"reply_msg":"Entering Extended Passive Mode (|||37100|)",
   * 	"data_channel.passive":true,
   * 	"data_channel.orig_h":"192.168.1.182",
   * 	"data_channel.resp_h":"192.168.1.231",
   * 	"data_channel.resp_p":37100
   * }
   */
  def fromFtp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromFtp(log.getAsJsonObject, schema)
    })
  }

  def fromFtp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "data_channel_passive", "data_channel.passive")
    newObject = replaceName(newObject, "data_channel_source_ip", "data_channel.orig_h")

    newObject = replaceName(newObject, "data_channel_destination_ip", "data_channel.resp_h")
    newObject = replaceName(newObject, "data_channel_destination_port", "data_channel.resp_p")

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def ftp():StructType = {

    var fields = Array(

      /* ts: Time when the command was sent.
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

      /* user: User name for the current FTP session.
       */
      StructField("user", StringType, nullable = true),

      /* password: Password for the current FTP session if captured.
       */
      StructField("password", StringType, nullable = true),

      /* command: Command given by the client.
       */
      StructField("command", StringType, nullable = true),

      /* arg: Argument for the command if one is given.
       */
      StructField("arg", StringType, nullable = true),

      /* mime_type: Sniffed mime type of file.
       */
      StructField("mime_type", StringType, nullable = true),

      /* file_size: Size of the file if the command indicates a file transfer.
       */
      StructField("file_size", IntegerType, nullable = true),

      /* reply_code: Reply code from the server in response to the command.
       */
      StructField("reply_code", IntegerType, nullable = true),

      /* reply_msg: Reply message from the server in response to the command.
       */
      StructField("reply_msg", StringType, nullable = true),

      /*** data_channel ***/

      /* data_channel.passive: Whether PASV mode is toggled for control channel.
       */
      StructField("data_channel_passive", BooleanType, nullable = true),

      /* data_channel.orig_h: The host that will be initiating the data connection.
       */
      StructField("data_channel_source_ip", StringType, nullable = true),

      /* data_channel.resp_h: The host that will be accepting the data connection.
       */
      StructField("data_channel_destination_ip", StringType, nullable = true),

      /* data_channel.resp_p: The port at which the acceptor is listening for the data connection.
       */
      StructField("data_channel_destination_port", IntegerType, nullable = true),

      /* fuid: File unique ID.
       */
      StructField("fuid", StringType, nullable = true)

    )

    StructType(fields)

  }
  /**
   * http (&log)
   *
   * The logging model is to log request/response pairs and all
   * relevant metadata together in a single record.
   *
   * {
   * 	"ts":1547687130.172944,
   * 	"uid":"CCNp8v1SNzY7v9d1Ih",
   * 	"id.orig_h":"10.178.98.102",
   * 	"id.orig_p":62995,
   * 	"id.resp_h":"17.253.5.203",
   * 	"id.resp_p":80,
   * 	"trans_depth":1,
   * 	"method":"GET",
   * 	"host":"ocsp.apple.com",
   * 	"uri":"/ocsp04-aaica02/ME4wTKADAgEAMEUwQzBBMAkGBSsOAwIaBQAEFNqvF+Za6oA4ceFRLsAWwEInjUhJBBQx6napI3Sl39T97qDBpp7GEQ4R7AIIUP1IOZZ86ns=",
   * 	"version":"1.1",
   * 	"user_agent":"com.apple.trustd/2.0",
   * 	"request_body_len":0,
   * 	"response_body_len":3735,
   * 	"status_code":200,
   * 	"status_msg":"OK",
   * 	"tags":[],
   * 	"resp_fuids":["F5zuip1tSwASjNAHy7"],
   * 	"resp_mime_types":["application/ocsp-response"]
   * }
   */
  def fromHttp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromHttp(log.getAsJsonObject, schema)
    })
  }

  def fromHttp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    json2Row(newObject, schema)

  }

  def http():StructType = {

    var fields = Array(

    /* ts: Timestamp for when the request happened.
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
    /* trans_depth: Represents the pipelined depth into the connection
     * of this request/response transaction.
     */
      StructField("trans_depth", IntegerType, nullable = false),

      /* method: Verb used in the HTTP request (GET, POST, HEAD, etc.).
       */
      StructField("method", StringType, nullable = true),

      /* host: Value of the HOST header.
       */
      StructField("host", StringType, nullable = true),

      /* uri: URI used in the request.
       */
      StructField("uri", StringType, nullable = true),

      /* referrer: Value of the “referer” header. The comment is deliberately misspelled
       * like the standard declares, but the name used here is “referrer” spelled correctly.
       */
      StructField("referrer", StringType, nullable = true),

      /* version: Value of the version portion of the request.
       */
      StructField("version", StringType, nullable = true),

      /* user_agent: Value of the User-Agent header from the client.
       */
      StructField("user_agent", StringType, nullable = true),

      /* origin: Value of the Origin header from the client.
       */
      StructField("origin", StringType, nullable = true),

      /* request_body_len: Actual uncompressed content size of the data transferred from the client.
       */
      StructField("request_body_len", IntegerType, nullable = true),

      /* response_body_len: Actual uncompressed content size of the data transferred from the server.
       */
      StructField("response_body_len",IntegerType, nullable = true),

      /* status_code: Status code returned by the server.
       */
      StructField("status_code", IntegerType, nullable = true),

      /* status_msg: Status message returned by the server.
       */
      StructField("status_msg", StringType, nullable = true),

      /* info_code: Last seen 1xx informational reply code returned by the server.
       */
      StructField("info_code", IntegerType, nullable = true),

      /* info_msg: Last seen 1xx informational reply message returned by the server.
       */
      StructField("info_msg", StringType, nullable = true),

      /* tags: A set of indicators of various attributes discovered and related to a
       * particular request/response pair.
       */
      StructField("tags", ArrayType(StringType), nullable = true),

      /* username: Username if basic-auth is performed for the request.
       */
      StructField("username", StringType, nullable = true),

      /* password: Password if basic-auth is performed for the request.
       */
      StructField("password", StringType, nullable = true),

      /* proxied: All of the headers that may indicate if the request was proxied.
       */
      StructField("proxied",ArrayType(StringType), nullable = true),

      /* orig_fuids: An ordered vector of file unique IDs.
       * Limited to HTTP::max_files_orig entries.
       */
      StructField("orig_fuids", ArrayType(StringType), nullable = true),

      /* orig_filenames: An ordered vector of filenames from the client.
       * Limited to HTTP::max_files_orig entries.
       */
      StructField("orig_filenames", ArrayType(StringType), nullable = true),

      /* orig_mime_types: An ordered vector of mime types.
       * Limited to HTTP::max_files_orig entries.
       */
      StructField("orig_mime_types", ArrayType(StringType), nullable = true),

      /* resp_fuids: An ordered vector of file unique IDs.
       * Limited to HTTP::max_files_resp entries.
       */
      StructField("resp_fuids", ArrayType(StringType), nullable = true),

      /* resp_filenames: An ordered vector of filenames from the server.
       * Limited to HTTP::max_files_resp entries.
       */
      StructField("resp_filenames", ArrayType(StringType), nullable = true),

      /* resp_mime_types: An ordered vector of mime types.
       * Limited to HTTP::max_files_resp entries.
       */
      StructField("resp_mime_types", ArrayType(StringType), nullable = true),

      /* client_header_names: The vector of HTTP header names sent by the client.
       * No header values are included here, just the header names.
       */
      StructField("client_header_names", ArrayType(StringType), nullable = true),

      /* server_header_names: The vector of HTTP header names sent by the server.
       * No header values are included here, just the header names.
       */
      StructField("server_header_names", ArrayType(StringType), nullable = true),

      /* cookie_vars: Variable names extracted from all cookies.
       */
      StructField("cookie_vars", ArrayType(StringType), nullable = true),

      /* uri_vars: Variable names from the URI.
       */
      StructField("uri_vars", ArrayType(StringType), nullable = true)

    )

    StructType(fields)

  }

  private def json2Row(jsonObject:JsonObject, schema:StructType):Row = {

    val values = schema.fields.map(field => {

      val fieldName = field.name
      val fieldType = field.dataType

      fieldType match {
        case ArrayType(LongType, true) =>
          getLongArray(jsonObject, fieldName, field.nullable)

        case ArrayType(LongType, false) =>
          getLongArray(jsonObject, fieldName, field.nullable)

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

  private def getLongArray(jsonObject:JsonObject, fieldName:String, nullable:Boolean):Array[Long] = {

    try {
      jsonObject.get(fieldName).getAsJsonArray
        .map(json => json.getAsLong)
        .toArray

    } catch {
      case _:Throwable =>
        if (nullable) Array.empty[Long]
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
