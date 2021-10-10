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
import de.kp.works.conf.WorksConf
import de.kp.works.json.JsonUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.JavaConversions._

object ZeekUtil {

  private val zeekCfg = WorksConf.getCfg(WorksConf.ZEEK_CONF)
  private val zeekKey = zeekCfg.getString("primaryKey")

  private val primaryKey = StructField(zeekKey, StringType, nullable = false)

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
    JsonUtil.json2Row(newObject, schema)

  }

  def capture_loss():StructType = {

    var fields = Array(

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

  }

  def dhcp():StructType = {

    var fields = Array(

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
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
        case _:Throwable => /* Do nothing */
      }

      new_ttls.add(interval)

    })

    newObject.add("dns_ttls", new_ttls)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def dns():StructType = {
/*
CREATE STREAM dns_stream (
RD BOOLEAN,
WITH (KAFKA_TOPIC='dns', VALUE_FORMAT='JSON');
 */
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

      /* RD: The Recursion Desired bit in a request message indicates that
       * the client wants recursive service for this query.
       */
      StructField("dns_rd", BooleanType, nullable = true),

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

  }
  /*
   * IMPORTANT: Despite the example above (from Filebeat),
   * the current Zeek documentation (v3.1.2) does not specify
   * a connection id
   */
  def files():StructType = {

    var fields = Array(

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
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
    JsonUtil.json2Row(newObject, schema)

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

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * intel (&log) - This log file is generated by Zeek's
   * intelligence framework as a more valuable information
   * compared to other (raw) log files.
   *
   * {
   * 	"ts":1573030980.989353,
   * 	"uid":"Ctefoj1tgOPt4D0EK2",
   * 	"id.orig_h":"192.168.1.1",
   * 	"id.orig_p":37598,
   * 	"id.resp_h":"198.41.0.4",
   * 	"id.resp_p":53,
   * 	"seen.indicator":"198.41.0.4",
   * 	"seen.indicator_type":"Intel::ADDR",
   * 	"seen.where":"Conn::IN_RESP",
   * 	"seen.node":"worker-1-2",
   * 	"matched":["Intel::ADDR"],
   * 	"sources":["ETPRO Rep: AbusedTLD Score: 127"]
   * }
   */
  def fromIntel(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromIntel(log.getAsJsonObject, schema)
    })
  }

  def fromIntel(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "seen_indicator", "seen.indicator")
    newObject = replaceName(newObject, "seen_indicator_type", "seen.indicator_type")

    newObject = replaceName(newObject, "seen_host", "seen.host")
    newObject = replaceName(newObject, "seen_where", "seen.where")

    newObject = replaceName(newObject, "seen_node", "seen.node")

    newObject = replaceName(newObject, "cif_tags", "cif.tags")
    newObject = replaceName(newObject, "cif_confidence", "cif.confidence")

    newObject = replaceName(newObject, "cif_source", "cif.source")
    newObject = replaceName(newObject, "cif_description", "cif.description")

    newObject = replaceName(newObject, "cif_firstseen", "cif.firstseen")
    newObject = replaceName(newObject, "cif_lastseen", "cif.lastseen")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }
  /**
   * This file transforms Zeek's Intel::Info format
   * into an Apache Spark compliant Intel schema.
   *
   * Sample:
   * {
   *   "ts":1320279566.452687,
   *   "uid":"C4llPsinsviGyNY45",
   *   "id.orig_h":"192.168.2.76",
   *   "id.orig_p":52026,
   *   "id.resp_h":"132.235.215.119",
   *   "id.resp_p":80,
   *   "seen.indicator":"www.reddit.com",
   *   "seen.indicator_type":"Intel::DOMAIN",
   *   "seen.where":"HTTP::IN_HOST_HEADER",
   *   "seen.node":"zeek",
   *   "matched":[
   *    "Intel::DOMAIN"
   *    ],
   *   "sources":[
   *    "my_special_source"
   *   ]
   * }
   */
  def intel():StructType = {

    var fields = Array(

      /* ts: Timestamp when the data was discovered.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: If a connection was associated with this intelligence hit,
       * this is the uid for the connection
       */
      StructField("uid", StringType, nullable = true)

    )
    /* id
     */
    fields = fields ++ conn_id_nullable()

    fields = fields ++ Array(

      /*** seen: Where the data was seen.
       *
       * Intel::Seen
       *
       * - indicator (string)
       * - indicator_type (enum)
       *
       *   Defined values are:
       *
       *   - Intel::ADDR (An IP address)
       *   - Intel::SUBNET (A subnet in CIDR notation)
       *   - Intel::URL (A complete URL without the prefix "http://")
       *   - Intel::SOFTWARE (Software name)
       *   - Intel::EMAIL (Email address)
       *   - Intel::DOMAIN (DNS domain name)
       *   - Intel::USER_NAME (A user name)
       *   - Intel::CERT_HASH (Certificate SHA-1 hash)
       *   - Intel::PUBKEY_HASH (Public key MD5 hash, formatted as hexadecimal digits delimited by colons)
       *   - Intel::FILE_HASH (present if base/frameworks/intel/files.zeek is loaded)
       *
       *                      File hash which is non-hash type specific. It’s up to the user to query for
       *                      any relevant hash types.
       *
       *   - Intel::FILE_NAME (present if base/frameworks/intel/files.zeek is loaded)
       *
       *                      File name. Typically with protocols with definite indications of a file name.
       *
       * - host (is defined, even it is not flagged with &log)
       * - where (enum)
       *
       *   Defined values are:
       *
       * - Intel::IN_ANYWHERE
       *   (A catchall value to represent data of unknown provenance.
       *
       *   Present if policy/frameworks/intel/seen/where-locations.zeek is loaded:
       *
       *   - Conn::IN_ORIG
       *   - Conn::IN_RESP
       *   - Files::IN_HASH
       *   - Files::IN_NAME
       *   - DNS::IN_REQUEST
       *   - DNS::IN_RESPONSE
       *   - HTTP::IN_HOST_HEADER
       *   - HTTP::IN_REFERRER_HEADER
       *   - HTTP::IN_USER_AGENT_HEADER
       *   - HTTP::IN_X_FORWARDED_FOR_HEADER
       *   - HTTP::IN_URL
       *   - SMTP::IN_MAIL_FROM
       *   - SMTP::IN_RCPT_TO
       *   - SMTP::IN_FROM
       *   - SMTP::IN_TO
       *   - SMTP::IN_CC
       *   - SMTP::IN_RECEIVED_HEADER
       *   - SMTP::IN_REPLY_TO
       *   - SMTP::IN_X_ORIGINATING_IP_HEADER
       *   - SMTP::IN_MESSAGE
       *   - SSH::IN_SERVER_HOST_KEY
       *   - SSL::IN_SERVER_NAME
       *   - SMTP::IN_HEADER
       *   - X509::IN_CERT
       *   - SMB::IN_FILE_NAME
       *   - SSH::SUCCESSFUL_LOGIN
       *
       * - node
       * - conn (not present, as it is not flagged with &log)
       * - uid (not present, as it is not flagged with &log)
       * - f (not present, as it is not flagged with &log)
       * - fuid(not present, as it is not flagged with &log)
       */

      /* seen.indicator: The string if the data is about a string.
       + This is the respective cyber observable value.
       */
      StructField("seen_indicator", StringType, nullable = true),

      /* seen.indicator_type: The type of data that the indicator represents.
       + The list is specified above and can be mapped onto STIX v2.
       */
      StructField("seen_indicator_type", StringType, nullable = true),

      /* seen.host: If the indicator type was Intel::ADDR, then this field
       * will be present.
       */
      StructField("seen_host", StringType, nullable = true),

      /* seen.where: Where the data was discovered.
       */
      StructField("seen_where", StringType, nullable = false),

      /* seen.node: The name of the node where the match was discovered.
       */
      StructField("seen_node", StringType, nullable = true),

      /* matched: Which indicator types matched. This is a list of
       * the respective indicator types
       */
      StructField("matched", ArrayType(StringType), nullable = false),

      /* sources: Sources which supplied data that resulted in this match.
       */
      StructField("sources", ArrayType(StringType), nullable = true),

      /* fuid: If a file was associated with this intelligence hit, this is the
       * uid for the file.
       */
      StructField("fuid", StringType, nullable = true),

      /* file_mime_type: A mime type if the intelligence hit is related to a file.
       * If the $f field is provided this will be automatically filled out.
       */
      StructField("file_mime_type", StringType, nullable = true),

      /* file_desc: Frequently files can be “described” to give a bit more context.
       * If the $f field is provided this field will be automatically filled out.
       */
      StructField("file_desc", StringType, nullable = true),

      /*** cif ***/

      /* cif.tags: CIF tags observations, examples for tags are botnet or exploit.
       */
      StructField("cif_tags", StringType, nullable = true),

      /* cif.confidence: In CIF Confidence details the degree of certainty of a given observation.
       */
      StructField("cif_confidence", DoubleType, nullable = true),

      /* cif.source: Source given in CIF.
       */
      StructField("cif_source", StringType, nullable = true),

      /* cif.description: Description given in CIF.
       */
      StructField("cif_description", StringType, nullable = true),

      /* cif.firstseen: First time the source observed the behavior.
       */
      StructField("cif_firstseen", StringType, nullable = true),

      /* cif.lastseen: Last time the source observed the behavior.
       */
      StructField("cif_lastseen", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

    /*
    Intel::Info
    Type
    record

    seen: Intel::Seen &log
    Where the data was seen.

    matched: Intel::TypeSet &log
    Which indicator types matched.

    sources: set [string] &log &default = {  } &optional
    Sources which supplied data that resulted in this match.

    fuid: string &log &optional
    (present if base/frameworks/intel/files.zeek is loaded)

    If a file was associated with this intelligence hit, this is the uid for the file.

    file_mime_type: string &log &optional
    (present if base/frameworks/intel/files.zeek is loaded)

    A mime type if the intelligence hit is related to a file. If the $f field is provided this will be automatically filled out.

    file_desc: string &log &optional
    (present if base/frameworks/intel/files.zeek is loaded)

    Frequently files can be “described” to give a bit more context. If the $f field is provided this field will be automatically filled out.

    cif: Intel::CIF &log &optional
    (present if policy/integration/collective-intel/main.zeek is loaded)
     */

  }

  /**
   * irc (&log)
   *
   * The logging model is to log IRC commands along with the associated response
   * and some additional metadata about the connection if it’s available.
   *
   * {
   * 	"ts":1387554250.647295,
   * 	"uid":"CNJBX5FQdL62VUUP1",
   * 	"id.orig_h":"10.180.156.249",
   * 	"id.orig_p":45921,
   * 	"id.resp_h":"38.229.70.20",
   * 	"id.resp_p":8000,
   * 	"command":"USER",
   * 	"value":"xxxxx",
   * 	"addl":"+iw xxxxx XxxxxxXxxx "
   * }
   */
  def fromIrc(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromIrc(log.getAsJsonObject, schema)
    })
  }

  def fromIrc(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def irc():StructType = {

    var fields = Array(

      /* ts: Timestamp for when the command was seen.
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

      /* nick: Nickname given for the connection.
       */
      StructField("nick", StringType, nullable = true),

      /* user: Username given for the connection.
       */
      StructField("user", StringType, nullable = true),

      /* command: Command given by the client.
       */
      StructField("command", StringType, nullable = true),

      /* value: Value for the command given by the client.
       */
      StructField("value", StringType, nullable = true),

      /* addl: Any additional data for the command.
       */
      StructField("addl", StringType, nullable = true),

      /* dcc_file_name: DCC filename requested.
       */
      StructField("dcc_file_name", StringType, nullable = true),

      /* dcc_file_size: Size of the DCC transfer as indicated by the sender.
       */
      StructField("dcc_file_size", IntegerType, nullable = true),

      /* dcc_mime_type: Sniffed mime type of the file.
       */
      StructField("dcc_mime_type", StringType, nullable = true),

      /* fuid: File unique ID.
       */
      StructField("fuid", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * kerberos (&log)
   *
   * {
   * 	"ts":1507565599.590346,
   * 	"uid":"C56Flhb4WQBNkfMOl",
   * 	"id.orig_h":"192.168.10.31",
   * 	"id.orig_p":49242,
   * 	"id.resp_h":"192.168.10.10",
   * 	"id.resp_p":88,
   * 	"request_type":"TGS",
   * 	"client":"RonHD/CONTOSO.LOCAL",
   * 	"service":"HOST/admin-pc",
   * 	"success":true,
   * 	"till":2136422885.0,
   * 	"cipher":"aes256-cts-hmac-sha1-96",
   * 	"forwardable":true,
   * 	"renewable":true
   * }
   */
  def fromKerberos(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromKerberos(log.getAsJsonObject, schema)
    })
  }

  def fromKerberos(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "from")
    newObject = replaceTime(newObject, "till")

    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def kerberos():StructType = {

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

      /* request_type: Request type - Authentication Service (“AS”) or
       * Ticket Granting Service (“TGS”)
       */
      StructField("request_type", StringType, nullable = true),

      /* client: Client.
       */
      StructField("client",StringType, nullable = true),

      /* service: Service.
       */
      StructField("service", StringType, nullable = true),

      /* success: Request result.
       */
      StructField("success", StringType, nullable = true),

      /* error_msg: Error message.
       */
      StructField("error_msg", StringType, nullable = true),

      /* from: Ticket valid from.
       */
      StructField("from", LongType, nullable = true),

      /* till: Ticket valid till.
       */
      StructField("till", LongType, nullable = true),

      /* cipher: Ticket encryption type.
       */
      StructField("cipher", StringType, nullable = true),

      /* forwardable: Forwardable ticket requested.
       */
      StructField("forwardable", StringType, nullable = true),

      /* renewable: Renewable ticket requested.
       */
      StructField("renewable", StringType, nullable = true),

      /* client_cert_subject: Subject of client certificate, if any.
       */
      StructField("client_cert_subject", StringType, nullable = true),

      /* client_cert_fuid: File unique ID of client cert, if any.
       */
      StructField("client_cert_fuid", StringType, nullable = true),

      /* server_cert_subject: Subject of server certificate, if any.
       */
      StructField("server_cert_subject", StringType, nullable = true),

      /* server_cert_fuid: File unique ID of server cert, if any.
       */
      StructField("server_cert_fuid", StringType, nullable = true),

      /* auth_ticket: Hash of ticket used to authorize request/transaction.
       */
      StructField("auth_ticket", StringType, nullable = true),

      /* new_ticket: Hash of ticket returned by the KDC.
       */
      StructField("new_ticket", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * modbus (&log)
   *
   * {
   * 	"ts":1352718265.222457,
   * 	"uid":"CpIIXl4DFGswmjH2bl",
   * 	"id.orig_h":"192.168.1.10",
   * 	"id.orig_p":64342,
   * 	"id.resp_h":"192.168.1.164",
   * 	"id.resp_p":502,
   * 	"func":"READ_COILS"
   * }
   */
  def fromModbus(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromModbus(log.getAsJsonObject, schema)
    })
  }

  def fromModbus(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def modbus():StructType = {

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

      /* func: The name of the function message that was sent.
       */
      StructField("func", StringType, nullable = true),

      /* exception: The exception if the response was a failure.
       */
      StructField("exception", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * mysql (&log)
   */
  def fromMysql(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromMysql(log.getAsJsonObject, schema)
    })
  }

  def fromMysql(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def mysql():StructType = {

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

      /* cmd: The command that was issued.
       */
      StructField("cmd", StringType, nullable = false),

      /* arg: The argument issued to the command.
       */
      StructField("arg", StringType, nullable = false),

      /* success: Did the server tell us that the command succeeded?
       */
      StructField("success", BooleanType, nullable = true),

      /* rows: The number of affected rows, if any.
       */
      StructField("rows", IntegerType, nullable = true),

      /* response: Server message, if any.
       */
      StructField("response", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * notice (&log)
   *
   * {
   * 	"ts":1320435875.879278,
   * 	"note":"SSH::Password_Guessing",
   * 	"msg":"172.16.238.1 appears to be guessing SSH passwords (seen in 30 connections).",
   * 	"sub":"Sampled servers:  172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136, 172.16.238.136",
   * 	"src":"172.16.238.1",
   * 	"peer_descr":"bro",
   * 	"actions":["Notice::ACTION_LOG"],
   * 	"suppress_for":3600.0,
   * 	"dropped":false
   * }
   */
  def fromNotice(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromNotice(log.getAsJsonObject, schema)
    })
  }

  def fromNotice(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "suppress_for")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "source_ip", "src")
    newObject = replaceName(newObject, "destination_ip", "dst")

    newObject = replaceName(newObject, "source_port", "p")

    newObject = replaceName(newObject, "country_code", "remote_location.country_code")
    newObject = replaceName(newObject, "region", "remote_location.region")
    newObject = replaceName(newObject, "city", "remote_location.city")

    newObject = replaceName(newObject, "latitude", "remote_location.latitude")
    newObject = replaceName(newObject, "longitude", "remote_location.longitude")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def notice():StructType = {

    var fields = Array(

      /* ts: An absolute time indicating when the notice occurred,
       * defaults to the current network time.
       */
      StructField("ts", LongType, nullable = true),

      /* uid: A connection UID which uniquely identifies the endpoints
       * concerned with the notice.
       */
      StructField("uid", StringType, nullable = true)

    )
    /* id
     */
    fields = fields ++ conn_id_nullable()

    fields = fields ++ Array(

      /* fuid: A file unique ID if this notice is related to a file. If the f field
       * is provided, this will be automatically filled out.
       */
      StructField("fuid", StringType, nullable = true),

      /* file_mime_type: A mime type if the notice is related to a file. If the f field
       * is provided, this will be automatically filled out.
       */
      StructField("file_mime_type", StringType, nullable = true),

      /* file_desc: Frequently files can be “described” to give a bit more context.
       * This field will typically be automatically filled out from an fa_file record.
       *
       * For example, if a notice was related to a file over HTTP, the URL of the request
       * would be shown.
       */
      StructField("file_desc", StringType, nullable = true),

      /* proto: The transport protocol. Filled automatically when either conn,
       * iconn or p is specified.
       */
      StructField("proto", StringType, nullable = true),

      /* note: The Notice::Type of the notice.
       */
      StructField("note", StringType, nullable = true),

      /* msg: The Notice::Type of the notice.
       */
      StructField("msg", StringType, nullable = true),

      /* sub: The human readable sub-message.
       */
      StructField("sub", StringType, nullable = true),

      /* src: Source address, if we don’t have a conn_id.
       */
      StructField("source_ip", StringType, nullable = true),

      /* dst: Destination address.
       */
      StructField("destination_ip", StringType, nullable = true),

      /* p: Associated port, if we don’t have a conn_id.
       *
       * This field is INTERPRETED as source_port as Zeek's documentation
       * does not clarify on this.
       */
      StructField("source_port", IntegerType, nullable = true),

      /* n: Associated count, or perhaps a status code.
       */
      StructField("n", IntegerType, nullable = true),

      /* peer_descr: Textual description for the peer that raised this notice,
       * including name, host address and port.
       */
      StructField("peer_descr", StringType, nullable = true),

      /* actions: The actions which have been applied to this notice.
       */
      StructField("actions", ArrayType(StringType), nullable = true),

      /* suppress_for: This field indicates the length of time that this unique notice
       * should be suppressed.
       */
      StructField("suppress_for", LongType, nullable = true),

      /*** remote_location: Add geographic data related to the “remote” host of the connection. ***/

      /* remote_location.country_code: The country code.
       */
      StructField("country_code", StringType, nullable = true),

      /* remote_location.region: The The region.
       */
      StructField("region", StringType, nullable = true),

      /* remote_location.city: The city.
       */
      StructField("city", StringType, nullable = true),

      /* remote_location.latitude: The latitude.
       */
      StructField("latitude", DoubleType, nullable = true),

      /* remote_location.longitude: longitude.
       */
      StructField("longitude", DoubleType, nullable = true),

      /* dropped: Indicate if the $src IP address was dropped and denied network access.
       */
      StructField("dropped", BooleanType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * ntlm (&log)
   *
   * NT LAN Manager (NTLM)
   *
   * {
   * 	"ts":1508959117.814467,
   * 	"uid":"CHphiNUKDC20fsy09",
   * 	"id.orig_h":"192.168.10.50",
   * 	"id.orig_p":46785,
   * 	"id.resp_h":"192.168.10.31",
   * 	"id.resp_p":445,
   * 	"username":"JeffV",
   * 	"hostname":"ybaARon55QykXrgu",
   * 	"domainname":"contoso.local",
   * 	"server_nb_computer_name":"VICTIM-PC",
   * 	"server_dns_computer_name":"Victim-PC.contoso.local",
   * 	"server_tree_name":"contoso.local"
   * }
   */
  def fromNtlm(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromNtlm(log.getAsJsonObject, schema)
    })
  }

  def fromNtlm(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def ntlm():StructType = {

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

      /* username: Username given by the client.
       */
      StructField("username", StringType, nullable = true),

      /* hostname: Hostname given by the client.
       */
      StructField("hostname", StringType, nullable = true),

      /* domainname: Domainname given by the client.
       */
      StructField("domainname", StringType, nullable = true),

      /* server_nb_computer_name: NetBIOS name given by the server in a CHALLENGE.
       */
      StructField("server_nb_computer_name", StringType, nullable = true),

      /* server_dns_computer_name: DNS name given by the server in a CHALLENGE.
       */
      StructField("server_dns_computer_name", StringType, nullable = true),

      /* server_tree_name: Tree name given by the server in a CHALLENGE.
       */
      StructField("server_tree_name", StringType, nullable = true),

      /* success: Indicate whether or not the authentication was successful.
       */
      StructField("success", BooleanType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * ocsp (&log)
   *
   * Online Certificate Status Protocol (OCSP). Only created if policy script is loaded.
   *
   */
  def fromOcsp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromOcsp(log.getAsJsonObject, schema)
    })
  }

  def fromOcsp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "revoketime")

    newObject = replaceTime(newObject, "thisUpdate")
    newObject = replaceTime(newObject, "nextUpdate")

    newObject = replaceName(newObject, "hash_algorithm", "hashAlgorithm")
    newObject = replaceName(newObject, "issuer_name_hash", "issuerNameHash")

    newObject = replaceName(newObject, "issuer_key_hash", "issuerKeyHash")
    newObject = replaceName(newObject, "serial_number", "serialNumber")

    newObject = replaceName(newObject, "cert_status", "certStatus")
    newObject = replaceName(newObject, "revoke_time", "revoketime")


    newObject = replaceName(newObject, "revoke_reason", "revokereason")
    newObject = replaceName(newObject, "update_this", "thisUpdate")

    newObject = replaceName(newObject, "update_next", "nextUpdate")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

 }

  def ocsp():StructType = {

    var fields = Array(

      /* ts: Time when the OCSP reply was encountered.
       */
      StructField("ts", LongType, nullable = false),

      /* id: File id of the OCSP reply.
       */
      StructField("id", StringType, nullable = false),

      /* hashAlgorithm: Hash algorithm used to generate issuerNameHash and issuerKeyHash.
       */
      StructField("hash_algorithm", StringType, nullable = false),

      /* issuerNameHash: Hash of the issuer’s distingueshed name.
       */
      StructField("issuer_name_hash", StringType, nullable = false),

      /* issuerKeyHash: Hash of the issuer’s public key.
       */
      StructField("issuer_key_hash", StringType, nullable = false),

      /* serialNumber: Serial number of the affected certificate.
       */
      StructField("serial_number", StringType, nullable = false),

      /* certStatus: Status of the affected certificate.
       */
      StructField("cert_status", StringType, nullable = false),

      /* revoketime: Time at which the certificate was revoked.
       */
      StructField("revoke_time", LongType, nullable = true),

      /* revokereason: Reason for which the certificate was revoked.
       */
      StructField("revoke_reason", StringType, nullable = true),

      /* thisUpdate: The time at which the status being shown is known
       * to have been correct.
       */
      StructField("update_this", LongType, nullable = false),

      /* nextUpdate: The latest time at which new information about the
       * status of the certificate will be available.
       */
      StructField("update_next", LongType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * pe (&log)
   *
   * Portable Executable (PE)
   */
  def fromPe(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromPe(log.getAsJsonObject, schema)
    })
  }

  def fromPe(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "compile_ts")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def pe():StructType = {

    var fields = Array(

      /* ts: Current timestamp.
       */
      StructField("ts", LongType, nullable = false),

      /* id: File id of this portable executable file.
       */
      StructField("id", StringType, nullable = false),

      /* machine: The target machine that the file was compiled for.
       */
      StructField("machine", StringType, nullable = true),

      /* compile_ts: The time that the file was created at.
       */
      StructField("compile_ts", LongType, nullable = true),

      /* os: The required operating system.
       */
      StructField("os", StringType, nullable = true),

      /* subsystem: The subsystem that is required to run this file.
       */
      StructField("subsystem", StringType, nullable = true),

      /* is_exe: Is the file an executable, or just an object file?
       */
      StructField("is_exe", BooleanType, nullable = true),

      /* is_64bit: Is the file a 64-bit executable?
       */
      StructField("is_64bit", BooleanType, nullable = true),

      /* uses_aslr: Does the file support Address Space Layout Randomization?
       */
      StructField("uses_aslr", BooleanType, nullable = true),

      /* uses_dep: Does the file support Data Execution Prevention?
       */
      StructField("uses_dep", BooleanType, nullable = true),

      /* uses_code_integrity: Does the file enforce code integrity checks?
       */
      StructField("uses_code_integrity", BooleanType, nullable = true),

      /* uses_seh: Does the file use structured exception handing?
       */
      StructField("uses_seh", BooleanType, nullable = true),

      /* has_import_table: Does the file have an import table?
       */
      StructField("has_import_table", BooleanType, nullable = true),

      /* has_export_table: Does the file have an export table?
       */
      StructField("has_export_table", BooleanType, nullable = true),

      /* has_cert_table: Does the file have an attribute certificate table?
       */
      StructField("has_cert_table", BooleanType, nullable = true),

      /* has_debug_data: Does the file have a debug table?
       */
      StructField("has_debug_data", BooleanType, nullable = true),

      /* section_names: The names of the sections, in order.
       */
      StructField("section_names", ArrayType(StringType), nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * radius (&log)
   *
   * RADIUS authentication attempts
   */
  def fromRadius(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromRadius(log.getAsJsonObject, schema)
    })
  }

  def fromRadius(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "ttl")

    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def radius():StructType = {

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

      /* username: The username, if present.
       */
      StructField("username", StringType, nullable = true),

      /* mac: MAC address, if present.
       */
      StructField("mac", StringType, nullable = true),

      /* framed_addr: The address given to the network access server, if present.
       * This is only a hint from the RADIUS server and the network access server
       * is not required to honor the address.
       */
      StructField("framed_addr", StringType, nullable = true),

      /* tunnel_client: Address (IPv4, IPv6, or FQDN) of the initiator end of the tunnel,
       * if present. This is collected from the Tunnel-Client-Endpoint attribute.
       */
      StructField("tunnel_client", StringType, nullable = true),

      /* connect_info: Connect info, if present.
       */
      StructField("connect_info", StringType, nullable = true),

      /* reply_msg: Reply message from the server challenge. This is frequently shown
       * to the user authenticating.
       */
      StructField("reply_msg", StringType, nullable = true),

      /* result: Successful or failed authentication.
       */
      StructField("result", StringType, nullable = true),

      /* ttl: The duration between the first request and either the “Access-Accept” message
       * or an error. If the field is empty, it means that either the request or response
       * was not seen.
       */
      StructField("ttl", LongType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * rdp (&log)
   *
   * RDP Analysis
   */
  def fromRdp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromRdp(log.getAsJsonObject, schema)
    })
  }

  def fromRdp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def rdp():StructType = {

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

      /* cookie: Cookie value used by the client machine. This is typically a username.
       */
      StructField("cookie", StringType, nullable = true),

      /* result: Status result for the connection. It’s a mix between RDP negotation failure
       * messages and GCC server create response messages.
       */
      StructField("result", StringType, nullable = true),

      /* security_protocol: Security protocol chosen by the server.
       */
      StructField("security_protocol", StringType, nullable = true),

      /* client_channels: The channels requested by the client.
       */
      StructField("client_channels", ArrayType(StringType), nullable = true),

      /* keyboard_layout: Keyboard layout (language) of the client machine.
       */
      StructField("keyboard_layout", StringType, nullable = true),

      /* client_build: RDP client version used by the client machine.
       */
      StructField("client_build", StringType, nullable = true),

      /* client_name: Name of the client machine.
       */
      StructField("client_name", StringType, nullable = true),

      /* client_dig_product_id: Product ID of the client machine.
       */
      StructField("client_dig_product_id", StringType, nullable = true),

      /* desktop_width: Desktop width of the client machine.
       */
      StructField("desktop_width", IntegerType, nullable = true),

      /* desktop_height: Desktop height of the client machine.
       */
      StructField("desktop_height", IntegerType, nullable = true),

      /* requested_color_depth: The color depth requested by the client in the
       * high_color_depth field.
       */
      StructField("requested_color_depth", StringType, nullable = true),

      /* cert_type: If the connection is being encrypted with native RDP encryption,
       * this is the type of cert being used.
       */
      StructField("cert_type", StringType, nullable = true),

      /* cert_count: The number of certs seen. X.509 can transfer an entire
       * certificate chain.
       */
      StructField("cert_count", IntegerType, nullable = true),

      /* cert_permanent: Indicates if the provided certificate or certificate
       * chain is permanent or temporary.
       */
      StructField("cert_permanent", BooleanType, nullable = true),

      /* encryption_level: Encryption level of the connection.
       */
      StructField("encryption_level", StringType, nullable = true),

      /* encryption_method: Encryption method of the connection.
       */
      StructField("encryption_method", StringType, nullable = true),

      /* ssl: Flag the connection if it was seen over SSL.
       */
      StructField("ssl", BooleanType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * rfb (&log)
   *
   * Remote Framebuffer (RFB)
   */
  def fromRfb(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromRfb(log.getAsJsonObject, schema)
    })
  }

  def fromRfb(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def rfb():StructType = {

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

      /* client_major_version: Major version of the client.
       */
      StructField("client_major_version", StringType, nullable = true),

      /* client_minor_version: Minor version of the client.
       */
      StructField("client_minor_version", StringType, nullable = true),

      /* server_major_version: Major version of the server.
       */
      StructField("server_major_version", StringType, nullable = true),

      /* server_minor_version: Minor version of the server.
       */
      StructField("server_minor_version", StringType, nullable = true),

      /* authentication_method: Identifier of authentication method used.
       */
      StructField("authentication_method", StringType, nullable = true),

      /* auth: Whether or not authentication was successful.
       */
      StructField("auth", BooleanType, nullable = true),

      /* share_flag: Whether the client has an exclusive or a shared session.
       */
      StructField("share_flag", BooleanType, nullable = true),

      /* desktop_name: Name of the screen that is being shared.
       */
      StructField("desktop_name", StringType, nullable = true),

      /* width: Width of the screen that is being shared.
       */
      StructField("width", IntegerType, nullable = true),

      /* height: Height of the screen that is being shared.
       */
      StructField("height", IntegerType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * sip (&log)
   *
   * Log from SIP analysis. The logging model is to log request/response pairs
   * and all relevant metadata together in a single record.
   *
   * {
   * 	"ts":1361916159.055464,
   * 	"uid":"CPRLCB4eWHdjP852Bk",
   * 	"id.orig_h":"172.16.133.19",
   * 	"id.orig_p":5060,
   * 	"id.resp_h":"74.63.41.218",
   * 	"id.resp_p":5060,
   * 	"trans_depth":0,
   * 	"method":"REGISTER",
   * 	"uri":"sip:newyork.voip.ms:5060",
   * 	"request_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
   * 	"request_to":"<sip:116954_Boston6@newyork.voip.ms>",
   * 	"response_from":"\u0022AppNeta\u0022 <sip:116954_Boston6@newyork.voip.ms>",
   * 	"response_to":"<sip:116954_Boston6@newyork.voip.ms>;tag=as023f66a5",
   * 	"call_id":"8694cd7e-976e4fc3-d76f6e38@172.16.133.19",
   * 	"seq":"4127 REGISTER",
   * 	"request_path":["SIP/2.0/UDP 172.16.133.19:5060"],
   * 	"response_path":["SIP/2.0/UDP 172.16.133.19:5060"],
   * 	"user_agent":"PolycomSoundStationIP-SSIP_5000-UA/3.2.4.0267",
   * 	"status_code":401,
   * 	"status_msg":"Unauthorized",
   * 	"request_body_len":0,
   * 	"response_body_len":0
   * }
   */
  def fromSip(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSip(log.getAsJsonObject, schema)
    })
  }

  def fromSip(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def sip():StructType = {

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
    fields = fields ++ conn_id

    fields = fields ++ Array(

      /* trans_depth: Represents the pipelined depth into the connection of this
       * request/response transaction.
       */
      StructField("trans_depth", IntegerType, nullable = false),

      /* method: Verb used in the SIP request (INVITE, REGISTER etc.).
       */
      StructField("method", StringType, nullable = true),

      /* uri: URI used in the request.
       */
      StructField("uri", StringType, nullable = true),

      /* date: Contents of the Date: header from the client.
       */
      StructField("date", StringType, nullable = true),

      /* request_from: Contents of the request From: header Note: The tag= value that’s usually
       * appended to the sender is stripped off and not logged.
       */
      StructField("request_from", StringType, nullable = true),

      /* request_to: Contents of the To: header.
       */
      StructField("request_to", StringType, nullable = true),

      /* response_from: Contents of the response From: header Note: The tag= value that’s usually
       * appended to the sender is stripped off and not logged.
       */
      StructField("response_from", StringType, nullable = true),

      /* response_to: Contents of the response To: header
       */
      StructField("response_to", StringType, nullable = true),

      /* reply_to: Contents of the Reply-To: header
       */
      StructField("reply_to", StringType, nullable = true),

      /* call_id: Contents of the Call-ID: header from the client
       */
      StructField("call_id", StringType, nullable = true),

      /* seq: Contents of the CSeq: header from the client
       */
      StructField("seq", StringType, nullable = true),

      /* subject: Contents of the Subject: header from the client
       */
      StructField("subject", StringType, nullable = true),

      /* request_path: The client message transmission path, as extracted from the headers.
       */
      StructField("request_path", ArrayType(StringType), nullable = true),

      /* response_path: The server message transmission path, as extracted from the headers.
       */
      StructField("response_path",  ArrayType(StringType), nullable = true),

      /* user_agent: Contents of the User-Agent: header from the client
       */
      StructField("user_agent", StringType, nullable = true),

      /* status_code: Status code returned by the server.
       */
      StructField("status_code", IntegerType, nullable = true),

      /* status_msg: Status message returned by the server.
       */
      StructField("status_msg", StringType, nullable = true),

      /* warning: Contents of the Warning: header
       */
      StructField("warning", StringType, nullable = true),

      /* request_body_len: Contents of the Content-Length: header from the client
       */
      StructField("request_body_len", IntegerType, nullable = true),

      /* response_body_len: Contents of the Content-Length: header from the server
       */
      StructField("response_body_len", IntegerType, nullable = true),

      /* content_type: Contents of the Content-Type: header from the server
       */
      StructField("content_type", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * smb_cmd (&log)
   *
   * SMB commands
   *
   * {
   * 	"ts":1361916332.020006,
   * 	"uid":"CbT8mpAXseu6Pt4R7",
   * 	"id.orig_h":"172.16.133.6",
   * 	"id.orig_p":1728,
   * 	"id.resp_h":"172.16.128.202",
   * 	"id.resp_p":445,
   * 	"command":"NT_CREATE_ANDX",
   * 	"argument":"\u005cbrowser",
   * 	"status":"SUCCESS",
   * 	"rtt":0.091141,
   * 	"version":"SMB1",
   * 	"tree":"\u005c\u005cJSRVR20\u005cIPC$",
   * 	"tree_service":"IPC",
   * 	"referenced_file.ts":1361916332.020006,
   * 	"referenced_file.uid":"CbT8mpAXseu6Pt4R7",
   * 	"referenced_file.id.orig_h":"172.16.133.6",
   * 	"referenced_file.id.orig_p":1728,
   * 	"referenced_file.id.resp_h":"172.16.128.202",
   * 	"referenced_file.id.resp_p":445,
   * 	"referenced_file.action":"SMB::FILE_OPEN",
   * 	"referenced_file.name":"\u005cbrowser",
   * 	"referenced_file.size":0
   * }
   */
  def fromSmbCmd(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSmbCmd(log.getAsJsonObject, schema)
    })
  }

  def fromSmbCmd(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "rtt")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "file_ts", "referenced_file.ts")
    newObject = replaceTime(newObject, "file_ts")

    newObject = replaceName(newObject, "file_uid", "referenced_file.uid")
    newObject = replaceName(newObject, "file_source_ip", "referenced_file.id.orig_h")

    newObject = replaceName(newObject, "file_source_port", "referenced_file.id.orig_p")
    newObject = replaceName(newObject, "file_destination_ip", "referenced_file.id.resp_h")

    newObject = replaceName(newObject, "file_destination_port", "referenced_file.id.resp_p")
    newObject = replaceName(newObject, "file_fuid", "referenced_file.fuid")

    newObject = replaceName(newObject, "file_action", "referenced_file.action")
    newObject = replaceName(newObject, "file_path", "referenced_file.path")

    newObject = replaceName(newObject, "file_name", "referenced_file.name")
    newObject = replaceName(newObject, "file_size", "referenced_file.size")

    newObject = replaceName(newObject, "file_prev_name", "referenced_file.prev_name")

    newObject = replaceName(newObject, "", "referenced_file.times.modified")
    newObject = replaceTime(newObject, "file_times_modified")

    newObject = replaceName(newObject, "file_times_accessed", "referenced_file.times.accessed")
    newObject = replaceTime(newObject, "file_times_accessed")

    newObject = replaceName(newObject, "file_times_created", "referenced_file.times.created")
    newObject = replaceTime(newObject, "file_times_created")

    newObject = replaceName(newObject, "file_times_changed", "referenced_file.times.changed")
    newObject = replaceTime(newObject, "file_times_changed")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def smb_cmd():StructType = {

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

      /* command: The command sent by the client.
       */
      StructField("command", StringType, nullable = false),

      /* sub_command: The subcommand sent by the client, if present.
       */
      StructField("sub_command", StringType, nullable = true),

      /* argument: Command argument sent by the client, if any.
       */
      StructField("argument", StringType, nullable = true),

      /* status: Server reply to the client’s command.
       */
      StructField("status", StringType, nullable = true),

      /* rtt: Round trip time from the request to the response.
       */
      StructField("rtt", LongType, nullable = true),

      /* version: Version of SMB for the command.
       */
      StructField("version", StringType, nullable = false),

      /* username: Authenticated username, if available.
       */
      StructField("username", StringType, nullable = true),

      /* tree: If this is related to a tree, this is the tree that was used
       * for the current command.
       */
      StructField("tree", StringType, nullable = true),

      /* tree_service: The type of tree (disk share, printer share,
       * named pipe, etc.).
       */
      StructField("tree_service", StringType, nullable = true),

      /*** referenced_file: If the command referenced a file, store it here. ***/

      /* referenced_file.ts: Time when the file was first discovered.
       */
      StructField("file_ts", LongType, nullable = true),

      /* referenced_file.uid: Unique ID of the connection the file was sent over.
       */
      StructField("file_uid", StringType, nullable = true),

      /*** referenced_file.id: ID of the connection the file was sent over. ***/

      /* referenced_file.id.orig_h: The originator’s IP address.
       */
      StructField("file_source_ip", StringType, nullable = true),

      /* referenced_file.id.orig_p: The originator’s port number.
       */
      StructField("file_source_port", IntegerType, nullable = true),

      /* referenced_file.id.resp_h: The responder’s IP address.
       */
      StructField("file_destination_ip", StringType, nullable = true),

      /* referenced_file.id.resp_p: The responder’s port number.
       */
      StructField("file_destination_port", IntegerType, nullable = true),

      /* referenced_file.fuid: Unique ID of the file.
       */
      StructField("file_fuid", StringType, nullable = true),

      /* referenced_file.action: Action this log record represents.
       */
      StructField("file_action", StringType, nullable = true),

      /* referenced_file.path: Path pulled from the tree this file was transferred to or from.
       */
      StructField("file_path", StringType, nullable = true),

      /* referenced_file.name: Filename if one was seen.
       */
      StructField("file_name", StringType, nullable = true),

      /* referenced_file.size: Total size of the file.
       */
      StructField("file_size", IntegerType, nullable = true),

      /* referenced_file.prev_name: If the rename action was seen,
       * this will be the file’s previous name.
       */
      StructField("file_prev_name", StringType, nullable = true),

      /*** times: Last time this file was modified. ***/

      /* referenced_file.times.modified: The time when data was last written to the file.
       */
      StructField("file_times_modified", LongType, nullable = true),

      /* referenced_file.times.accessed: The time when the file was last accessed.
       */
      StructField("file_times_accessed", LongType, nullable = true),

      /* referenced_file.times.created: The time the file was created.
       */
      StructField("file_times_created", LongType, nullable = true),

      /* referenced_file.times.changed: The time when the file was last modified.
       */
      StructField("file_times_changed", LongType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * smb_files (&log)
   *
   * SMB files
   *
   * {
   * 	"ts":1507565599.576942,
   * 	"uid":"C9YAaEzWLL62yWMn5",
   * 	"id.orig_h":"192.168.10.31",
   * 	"id.orig_p":49239,
   * 	"id.resp_h":"192.168.10.30",
   * 	"id.resp_p":445,
   * 	"action":"SMB::FILE_OPEN",
   * 	"path":"\u005c\u005cadmin-pc\u005cADMIN$",
   * 	"name":"PSEXESVC.exe",
   * 	"size":0,
   * 	"times.modified":1507565599.607777,
   * 	"times.accessed":1507565599.607777,
   * 	"times.created":1507565599.607777,
   * 	"times.changed":1507565599.607777
   * }
   */
  def fromSmbFiles(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSmbFiles(log.getAsJsonObject, schema)
    })
  }

  def fromSmbFiles(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")

    newObject = replaceTime(newObject, "times.modified")
    newObject = replaceTime(newObject, "times.accessed")

    newObject = replaceTime(newObject, "times.created")
    newObject = replaceTime(newObject, "times.changed")

    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "times_modified", "times.modified")
    newObject = replaceName(newObject, "times_accessed", "times.accessed")

    newObject = replaceName(newObject, "times_created", "times.created")
    newObject = replaceName(newObject, "times_changed", "times.changed")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def smb_files():StructType = {

    var fields = Array(

      /* ts: Time when the file was first discovered.
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

      /* fuid: Unique ID of the file.
       */
      StructField("fuid", StringType, nullable = true),

      /* action: Action this log record represents.
       */
      StructField("action", StringType, nullable = true),

      /* path: Path pulled from the tree this file was transferred to or from.
       */
      StructField("path", StringType, nullable = true),

      /* name: Filename if one was seen.
       */
      StructField("name", StringType, nullable = true),

      /* size: Total size of the file.
       */
      StructField("size", IntegerType, nullable = true),

      /* prev_name: If the rename action was seen, this will be the file’s previous name.
       */
      StructField("prev_name", StringType, nullable = true),

      /*** times: Last time this file was modified. ***/

      /* times.modified: The time when data was last written to the file.
       */
      StructField("times_modified", LongType, nullable = true),

      /* times.accessed: The time when the file was last accessed.
       */
      StructField("times_accessed", LongType, nullable = true),

      /* times.created: The time the file was created.
       */
      StructField("times_created", LongType, nullable = true),

      /* times.changed: The time when the file was last modified.
       */
      StructField("times_changed", LongType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * smb_mapping (&log)
   *
   * SMB Trees
   *
   * {
   * 	"ts":1507565599.576613,
   * 	"uid":"C9YAaEzWLL62yWMn5",
   * 	"id.orig_h":"192.168.10.31",
   * 	"id.orig_p":49239,
   * 	"id.resp_h":"192.168.10.30",
   * 	"id.resp_p":445,
   * 	"path":"\u005c\u005cadmin-pc\u005cADMIN$",
   * 	"share_type":"DISK"
   * }
   */
  def fromSmbMapping(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSmbMapping(log.getAsJsonObject, schema)
    })
  }

  def fromSmbMapping(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def smb_mapping():StructType = {

    var fields = Array(

      /* ts: Time when the tree was mapped.
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

      /* path: Name of the tree path.
       */
      StructField("path", StringType, nullable = true),

      /* service: The type of resource of the tree (disk share, printer share,
       * named pipe, etc.).
       */
      StructField("service", StringType, nullable = true),

      /* native_file_system: File system of the tree.
       */
      StructField("native_file_system", StringType, nullable = true),

      /* share_type: If this is SMB2, a share type will be included. For SMB1,
       * the type of share will be deduced and included as well.
       */
      StructField("share_type", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * smtp (&log)
   *
   * SMTP transactions
   *
   * {
   * 	"ts":1543877987.381899,
   * 	"uid":"CWWzPB3RjqhFf528c",
   * 	"id.orig_h":"192.168.1.10",
   * 	"id.orig_p":33782,
   * 	"id.resp_h":"192.168.1.9",
   * 	"id.resp_p":25,
   * 	"trans_depth":1,
   * 	"helo":"EXAMPLE.COM",
   * 	"last_reply":"220 2.0.0 SMTP server ready",
   * 	"path":["192.168.1.9"],
   * 	"tls":true,
   * 	"fuids":[],
   * 	"is_webmail":false
   * }
   */
  def fromSmtp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSmtp(log.getAsJsonObject, schema)
    })
  }

  def fromSmtp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def smtp():StructType = {

    var fields = Array(

      /* ts: Time when the message was first seen.
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

      /* trans_depth: A count to represent the depth of this message transaction
       * in a single connection where multiple messages were transferred.
       */
      StructField("trans_depth", IntegerType, nullable = false),

      /* helo: Contents of the Helo header.
       */
      StructField("helo", StringType, nullable = true),

      /* mailfrom: Email addresses found in the From header.
       */
      StructField("mailfrom", StringType, nullable = true),

      /* rcptto: Email addresses found in the Rcpt header.
       */
      StructField("rcptto", ArrayType(StringType), nullable = true),

      /* date: Contents of the Date header.
       */
      StructField("date", StringType, nullable = true),

      /* from: Contents of the From header.
       */
      StructField("from", StringType, nullable = true),

      /* to: Contents of the To header.
       */
      StructField("to", ArrayType(StringType), nullable = true),

      /* cc: Contents of the CC header.
       */
      StructField("cc", ArrayType(StringType), nullable = true),

      /* reply_to: Contents of the ReplyTo header.
       */
      StructField("reply_to", StringType, nullable = true),

      /* msg_id: Contents of the MsgID header.
       */
      StructField("msg_id", StringType, nullable = true),

      /* in_reply_to: Contents of the In-Reply-To header.
       */
      StructField("in_reply_to", StringType, nullable = true),

      /* subject: Contents of the Subject header.
       */
      StructField("subject", StringType, nullable = true),

      /* x_originating_ip: Contents of the X-Originating-IP header.
       */
      StructField("x_originating_ip", StringType, nullable = true),

      /* first_received: Contents of the first Received header.
       */
      StructField("first_received", StringType, nullable = true),

      /* second_received: Contents of the second Received header.
       */
      StructField("second_received", StringType, nullable = true),

      /* last_reply: The last message that the server sent to the client.
       */
      StructField("last_reply", StringType, nullable = true),

      /* path: The message transmission path, as extracted from the headers.
       */
      StructField("path", ArrayType(StringType), nullable = true),

      /* user_agent: Value of the User-Agent header from the client.
       */
      StructField("user_agent", StringType, nullable = true),

      /* tls: Indicates that the connection has switched to using TLS.
       */
      StructField("tls", BooleanType, nullable = true),

      /* fuids: An ordered vector of file unique IDs seen attached to the message.
       */
      StructField("fuids", ArrayType(StringType), nullable = true),

      /* is_webmail: Boolean indicator of if the message was sent through a webmail interface.
       */
      StructField("is_webmail", BooleanType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }

  /**
   * snmp (&log)
   *
   * SNMP messages
   *
   * {
   * 	"ts":1543877948.916584,
   * 	"uid":"CnKW1B4w9fpRa6Nkf2",
   * 	"id.orig_h":"192.168.1.2",
   * 	"id.orig_p":59696,
   * 	"id.resp_h":"192.168.1.1",
   * 	"id.resp_p":161,
   * 	"duration":7.849924,
   * 	"version":"2c",
   * 	"community":"public",
   * 	"get_requests":0,
   * 	"get_bulk_requests":0,
   * 	"get_responses":8,
   * 	"set_requests":0,
   * 	"up_since":1543631204.766508
   * }
   */
  def fromSnmp(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSnmp(log.getAsJsonObject, schema)
    })
  }

  def fromSnmp(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceTime(newObject, "up_since")

    newObject = replaceInterval(newObject, "duration")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def snmp():StructType = {

    var fields = Array(

      /* ts: Timestamp of first packet belonging to the SNMP session.
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

      /* duration: The amount of time between the first packet beloning to the
       * SNMP session and the latest one seen.
       */
      StructField("duration", LongType, nullable = true),

      /* version: The version of SNMP being used.
       */
      StructField("version", StringType, nullable = false),

      /* community: The community string of the first SNMP packet associated with the session.
       * This is used as part of SNMP’s (v1 and v2c) administrative/security framework.
       *
       * See RFC 1157 or RFC 1901.
       */
      StructField("community", StringType, nullable = true),

      /* get_requests: The number of variable bindings in GetRequest/GetNextRequest
       * PDUs seen for the session.
       */
      StructField("get_requests", IntegerType, nullable = true),

      /* get_bulk_requests: The number of variable bindings in GetBulkRequest PDUs seen
       * for the session.
       */
      StructField("get_bulk_requests", IntegerType, nullable = true),

      /* get_responses: The number of variable bindings in GetResponse/Response PDUs
       * seen for the session.
       */
      StructField("get_responses", IntegerType, nullable = true),

      /* set_requests: The number of variable bindings in SetRequest PDUs seen for the session.
       */
      StructField("set_requests", IntegerType, nullable = true),

      /* display_string: A system description of the SNMP responder endpoint.
       */
      StructField("display_string", StringType, nullable = true),

      /* up_since: The time at which the SNMP responder endpoint claims it’s been up since.
       */
      StructField("up_since", LongType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * socks (&log)
   *
   * SOCKS proxy requests
   *
   * {
   * 	"ts":1566508093.09494,
   * 	"uid":"Cmz4Cb4qCw1hGqYw1c",
   * 	"id.orig_h":"127.0.0.1",
   * 	"id.orig_p":35368,
   * 	"id.resp_h":"127.0.0.1",
   * 	"id.resp_p":8080,
   * 	"version":5,
   * 	"status":"succeeded",
   * 	"request.name":"www.google.com",
   * 	"request_p":443,
   * 	"bound.host":"0.0.0.0",
   * 	"bound_p":0
   * }
   */
  def fromSocks(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSocks(log.getAsJsonObject, schema)
    })
  }

  def fromSocks(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "request_host", "request.host")
    newObject = replaceName(newObject, "request_name", "request.name")
    newObject = replaceName(newObject, "request_port", "request_p")

    newObject = replaceName(newObject, "bound_host", "bound.host")
    newObject = replaceName(newObject, "bound_name", "bound.name")
    newObject = replaceName(newObject, "bound_port", "bound_p")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def socks():StructType = {

    var fields = Array(

      /* ts: Time when the proxy connection was first detected.
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

      /* version: Protocol version of SOCKS.
       */
      StructField("version", IntegerType, nullable = false),

      /* user: Username used to request a login to the proxy.
       */
      StructField("user", StringType, nullable = true),

      /* password: Password used to request a login to the proxy.
       */
      StructField("password", StringType, nullable = true),

      /* status: Server status for the attempt at using the proxy.
       */
      StructField("status", StringType, nullable = true),

      /* request: Client requested SOCKS address. Could be an address, a name or both. */

      /* request.host:
       */
      StructField("request_host", StringType, nullable = true),

      /* request.name:
       */
      StructField("request_name", StringType, nullable = true),

      /* request_p: Client requested port.
       */
      StructField("request_port", IntegerType, nullable = true),

      /* bound: Server bound address. Could be an address, a name or both. */

      /* bound.host:
       */
      StructField("bound_host", StringType, nullable = true),

      /* bound.name:
       */
      StructField("bound_name", StringType, nullable = true),

      /* bound_p: Server bound port.
       */
      StructField("bound_port", IntegerType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * ssh (&log)
   *
   * {
   * 	"ts":1562527532.904291,
   * 	"uid":"CajWfz1b3qnnWT0BU9",
   * 	"id.orig_h":"192.168.1.2",
   * 	"id.orig_p":48380,
   * 	"id.resp_h":"192.168.1.1",
   * 	"id.resp_p":22,
   * 	"version":2,
   * 	"auth_success":false,
   * 	"auth_attempts":2,
   * 	"client":"SSH-2.0-OpenSSH_7.9p1 Ubuntu-10",
   * 	"server":"SSH-2.0-OpenSSH_6.6.1p1 Debian-4~bpo70+1",
   * 	"cipher_alg":"chacha20-poly1305@openssh.com",
   * 	"mac_alg":"umac-64-etm@openssh.com",
   * 	"compression_alg":"none",
   * 	"kex_alg":"curve25519-sha256@libssh.org",
   * 	"host_key_alg":"ecdsa-sha2-nistp256",
   * 	"host_key":"86:71:ac:9c:35:1c:28:29:05:81:48:ec:66:67:de:bd"
   * }
   */
  def fromSsh(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSsh(log.getAsJsonObject, schema)
    })
  }

  def fromSsh(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "country_code", "remote_location.country_code")
    newObject = replaceName(newObject, "region", "remote_location.region")
    newObject = replaceName(newObject, "city", "remote_location.city")

    newObject = replaceName(newObject, "latitude", "remote_location.latitude")
    newObject = replaceName(newObject, "longitude", "remote_location.longitude")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def ssh():StructType = {

    var fields = Array(

      /* ts: Time when the SSH connection began.
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

      /* version: SSH major version (1 or 2)
       */
      StructField("version", IntegerType, nullable = false),

      /* auth_success: Authentication result (T=success, F=failure, unset=unknown)
       */
      StructField("auth_success", BooleanType, nullable = true),

      /* auth_attempts: The number of authentication attemps we observed. There’s always at least
       * one, since some servers might support no authentication at all. It’s important to note that
       * not all of these are failures, since some servers require two-factor auth (e.g. password AND
       * pubkey)
       */
      StructField("auth_attempts", IntegerType, nullable = true),

      /* direction: Direction of the connection. If the client was a local host logging into an
       * external host, this would be OUTBOUND. INBOUND would be set for the opposite situation.
       */
      StructField("direction", StringType, nullable = true),

      /* client: The client’s version string.
       */
      StructField("client", StringType, nullable = true),

      /* server: The server’s version string.
       */
      StructField("server", StringType, nullable = true),

      /* cipher_alg: The encryption algorithm in use.
       */
      StructField("cipher_alg", StringType, nullable = true),

      /* mac_alg: The signing (MAC) algorithm in use.
       */
      StructField("mac_alg", StringType, nullable = true),

      /* compression_alg: The compression algorithm in use.
       */
      StructField("compression_alg", StringType, nullable = true),

      /* kex_alg: The key exchange algorithm in use.
       */
      StructField("kex_alg", StringType, nullable = true),

      /* host_key_alg: The server host key’s algorithm.
       */
      StructField("host_key_alg", StringType, nullable = true),

      /* host_key: The server’s key fingerprint.
       */
      StructField("host_key", StringType, nullable = true),

      /*** remote_location: Add geographic data related to the “remote” host of the connection. ***/

      /* remote_location.country_code: The country code.
       */
      StructField("country_code", StringType, nullable = true),

      /* remote_location.region: The The region.
       */
      StructField("region", StringType, nullable = true),

      /* remote_location.city: The city.
       */
      StructField("city", StringType, nullable = true),

      /* remote_location.latitude: The latitude.
       */
      StructField("latitude", DoubleType, nullable = true),

      /* remote_location.longitude: longitude.
       */
      StructField("longitude", DoubleType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * ssl (&log)
   *
   * This logs information about the SSL/TLS handshaking
   * and encryption establishment process.
   *
   * {
   *  "ts":1547688736.805088,
   * 	"uid":"CAOvs1BMFCX2Eh0Y3",
   * 	"id.orig_h":"10.178.98.102",
   * 	"id.orig_p":63199,
   * 	"id.resp_h":"35.199.178.4",
   * 	"id.resp_p":9243,
   * 	"version":"TLSv12",
   * 	"cipher":"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
   * 	"curve":"secp256r1",
   * 	"server_name":"dd625ffb4fc54735b281862aa1cd6cd4.us-west1.gcp.cloud.es.io",
   * 	"resumed":false,
   * 	"established":true,
   * 	"cert_chain_fuids":["FebkbHWVCV8rEEEne","F4BDY41MGUBT6URZMd","FWlfEfiHVkv8evDL3"],
   * 	"client_cert_chain_fuids":[],
   * 	"subject":"CN=*.gcp.cloud.es.io,O=Elasticsearch\u005c, Inc.,L=Mountain View,ST=California,C=US",
   * 	"issuer":"CN=DigiCert SHA2 Secure Server CA,O=DigiCert Inc,C=US",
   * 	"validation_status":"ok"
   * }
   */
  def fromSsl(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSsl(log.getAsJsonObject, schema)
    })
  }

  def fromSsl(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    newObject = replaceName(newObject, "notary_first_seen", "notary.first_seen")
    newObject = replaceName(newObject, "notary_last_seen", "notary.last_seen")

    newObject = replaceName(newObject, "notary_times_seen", "notary.times_seen")
    newObject = replaceName(newObject, "notary_valid", "notary.valid")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def ssl():StructType = {

    var fields = Array(

      /* ts: Time when the SSL connection was first detected.
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

      /* version: SSL/TLS version that the server chose.
       */
      StructField("version", StringType, nullable = true),

      /* cipher: SSL/TLS cipher suite that the server chose.
       */
      StructField("cipher", StringType, nullable = true),

      /* curve: Elliptic curve the server chose when using ECDH/ECDHE.
       */
      StructField("curve", StringType, nullable = true),

      /* server_name: Value of the Server Name Indicator SSL/TLS extension.
       * It indicates the server name that the client was requesting.
       */
      StructField("server_name", StringType, nullable = true),

      /* resumed: Flag to indicate if the session was resumed reusing the
       * key material exchanged in an earlier connection.
       */
      StructField("resumed", BooleanType, nullable = true),

      /* last_alert: Last alert that was seen during the connection.
       */
      StructField("last_alert", StringType, nullable = true),

      /* next_protocol: Next protocol the server chose using the application
       * layer next protocol extension, if present.
       */
      StructField("next_protocol", StringType, nullable = true),

      /* established: Flag to indicate if this ssl session has been established
       * successfully, or if it was aborted during the handshake.
       */
      StructField("established", BooleanType, nullable = true),

      /* cert_chain_fuids: An ordered vector of all certificate file unique IDs
       * for the certificates offered by the server.
       */
      StructField("cert_chain_fuids", ArrayType(StringType), nullable = true),

      /* client_cert_chain_fuids: An ordered vector of all certificate file unique IDs
       * for the certificates offered by the client.
       */
      StructField("client_cert_chain_fuids", ArrayType(StringType), nullable = true),

      /* subject: Subject of the X.509 certificate offered by the server.
       */
      StructField("subject", StringType, nullable = true),

      /* issuer: Subject of the signer of the X.509 certificate offered by the server.
       */
      StructField("issuer", StringType, nullable = true),

      /* client_subject: Subject of the X.509 certificate offered by the client.
       */
      StructField("client_subject", StringType, nullable = true),

      /* client_issuer: Subject of the signer of the X.509 certificate offered by the client.
       */
      StructField("client_issuer", StringType, nullable = true),

      /* validation_status: Result of certificate validation for this connection.
       */
      StructField("validation_status", StringType, nullable = true),

      /* ocsp_status: Result of ocsp validation for this connection.
       */
      StructField("ocsp_status", StringType, nullable = true),

      /* valid_ct_logs: Number of different Logs for which valid SCTs were
       * encountered in the connection.
       */
      StructField("valid_ct_logs", IntegerType, nullable = true),

      /* valid_ct_operators: Number of different Log operators of which valid
       * SCTs were encountered in the connection.
       */
      StructField("valid_ct_operators", IntegerType, nullable = true),

      /*** notary: A response from the ICSI certificate notary. ***/

      /* notary.first_seen:
       */
      StructField("notary_first_seen", IntegerType, nullable = true),

      /* notary.last_seen:
       */
      StructField("notary_last_seen", IntegerType, nullable = true),

      /* notary.times_seen:
       */
      StructField("notary_times_seen", IntegerType, nullable = true),

      /* notary.valid:
       */
      StructField("notary_valid", BooleanType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * stats (&log)
   *
   * {
   * 	"ts":1476605878.714844,
   * 	"peer":"bro",
   * 	"mem":94,
   * 	"pkts_proc":296,
   * 	"bytes_recv":39674,
   * 	"events_proc":723,
   * 	"events_queued":728,
   * 	"active_tcp_conns":1,
   * 	"active_udp_conns":3,
   * 	"active_icmp_conns":0,
   * 	"tcp_conns":6,
   * 	"udp_conns":36,
   * 	"icmp_conns":2,
   * 	"timers":797,
   * 	"active_timers":38,
   * 	"files":0,
   * 	"active_files":0,
   * 	"dns_requests":0,
   * 	"active_dns_requests":0,
   * 	"reassem_tcp_size":0,
   * 	"reassem_file_size":0,
   * 	"reassem_frag_size":0,
   * 	"reassem_unknown_size":0
   * }
   */
  def fromStats(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromStats(log.getAsJsonObject, schema)
    })
  }

  def fromStats(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceInterval(newObject, "pkt_lag")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def stats():StructType = {

    var fields = Array(

      /* ts: Timestamp for the measurement.
       */
      StructField("ts", LongType, nullable = false),

      /* peer: Peer that generated this log. Mostly for clusters.
       */
      StructField("peer", StringType, nullable = false),

      /* mem: Amount of memory currently in use in MB.
       */
      StructField("mem", IntegerType, nullable = false),

      /* pkts_proc: Number of packets processed since the last stats interval.
       */
      StructField("pkts_proc", IntegerType, nullable = false),

      /* bytes_recv: Number of bytes received since the last stats interval
       * if reading live traffic.
       */
      StructField("bytes_recv", IntegerType, nullable = false),

      /* pkts_dropped: Number of packets dropped since the last stats
       * interval if reading live traffic.
       */
      StructField("pkts_dropped", IntegerType, nullable = true),

      /* pkts_link: Number of packets seen on the link since the last
       * stats interval if reading live traffic.
       */
      StructField("pkts_link", IntegerType, nullable = true),

      /* pkt_lag: Lag between the wall clock and packet timestamps
       * if reading live traffic.
       */
      StructField("pkt_lag", LongType, nullable = true),

      /* events_proc: Number of events processed since the last stats
       * interval.
       */
      StructField("events_proc", IntegerType, nullable = false),

      /* events_queued: Number of events that have been queued since
       * the last stats interval.
       */
      StructField("events_queued", IntegerType, nullable = false),

      /* active_tcp_conns: TCP connections currently in memory.
       */
      StructField("active_tcp_conns", IntegerType, nullable = false),

      /* active_udp_conns: UDP connections currently in memory.
       */
      StructField("active_udp_conns", IntegerType, nullable = false),

      /* active_icmp_conns: ICMP connections currently in memory.
       */
      StructField("active_icmp_conns", IntegerType, nullable = false),

      /* tcp_conns: TCP connections seen since last stats interval.
       */
      StructField("tcp_conns", IntegerType, nullable = false),

      /* udp_conns: UDP connections seen since last stats interval.
       */
      StructField("udp_conns", IntegerType, nullable = false),

      /* icmp_conns: ICMP connections seen since last stats interval.
       */
      StructField("icmp_conns", IntegerType, nullable = false),

      /* timers: Number of timers scheduled since last stats interval.
       */
      StructField("timers", IntegerType, nullable = false),

      /* active_timers: Current number of scheduled timers.
       */
      StructField("active_timers", IntegerType, nullable = false),

      /* files: Number of files seen since last stats interval.
       */
      StructField("files", IntegerType, nullable = false),

      /* active_files: Current number of files actively being seen.
       */
      StructField("active_files", IntegerType, nullable = false),

      /* dns_requests: Number of DNS requests seen since last stats interval.
       */
      StructField("dns_requests", IntegerType, nullable = false),

      /* active_dns_requests: Current number of DNS requests awaiting a reply.
       */
      StructField("active_dns_requests", IntegerType, nullable = false),

      /* reassem_tcp_size: Current size of TCP data in reassembly.
       */
      StructField("reassem_tcp_size", IntegerType, nullable = false),

      /* reassem_file_size: Current size of File data in reassembly.
       */
      StructField("reassem_file_size", IntegerType, nullable = false),

      /* reassem_frag_size: Current size of packet fragment data in reassembly.
       */
      StructField("reassem_frag_size", IntegerType, nullable = false),

      /* reassem_unknown_size: Current size of unknown data in reassembly
       * (this is only PIA buffer right now).
       */
      StructField("reassem_unknown_size", IntegerType, nullable = false)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * syslog (&log)
   *
   * Syslog messages.
   */
  def fromSyslog(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromSyslog(log.getAsJsonObject, schema)
    })
  }

  def fromSyslog(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

 def syslog():StructType = {

    var fields = Array(

      /* ts: Timestamp when the syslog message was seen.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: A unique identifier of the connection.
       */
      StructField("uid", StringType, nullable = false)

    )
    /* id
     */
    fields = fields ++ conn_id()

    /* proto: Protocol over which the message was seen.
     */
   fields = fields ++ Array(

     StructField("proto", StringType, nullable = false),

     /* facility: Syslog facility for the message.
      */
     StructField("facility", StringType, nullable = false),

     /* severity: Syslog severity for the message.
      */
     StructField("severity", StringType, nullable = false),

     /* message: The plain text message.
      */
     StructField("message", StringType, nullable = false)

   )

   fields = Array(primaryKey) ++ fields
   StructType(fields)

 }

  /**
   * traceroute (&log)
   *
   * {
   * 	"ts":1361916158.650605,
   * 	"src":"192.168.1.1",
   * 	"dst":"8.8.8.8",
   * 	"proto":"udp"
   * }
   */
  def fromTraceroute(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromTraceroute(log.getAsJsonObject, schema)
    })
  }

  def fromTraceroute(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")

    newObject = replaceName(newObject, "source_ip", "src")
    newObject = replaceName(newObject, "destination_ip", "dst")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def traceroute():StructType = {

    var fields = Array(

      /* ts: Timestamp
       */
      StructField("ts", LongType, nullable = false),

      /* src: Address initiating the traceroute.
       */
      StructField("source_ip", StringType, nullable = false),

      /* dst: Destination address of the traceroute.
       */
      StructField("destination_ip", StringType, nullable = false),

      /* proto: Protocol used for the traceroute.
       */
      StructField("proto", StringType, nullable = false)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * tunnel (&log)
   *
   * This log handles the tracking/logging of tunnels (e.g. Teredo, AYIYA, or IP-in-IP
   * such as 6to4 where “IP” is either IPv4 or IPv6).
   *
   * For any connection that occurs over a tunnel, information about its encapsulating
   * tunnels is also found in the tunnel field of connection.
   *
   * {
   * 	"ts":1544405666.743509,
   * 	"id.orig_h":"132.16.146.79",
   * 	"id.orig_p":0,
   * 	"id.resp_h":"132.16.110.133",
   * 	"id.resp_p":8080,
   * 	"tunnel_type":"Tunnel::HTTP",
   * 	"action":"Tunnel::DISCOVER"
   * }
   */
  def fromTunnel(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromTunnel(log.getAsJsonObject, schema)
    })
  }

  def fromTunnel(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def tunnel():StructType = {

    var fields = Array(

      /* ts: Time at which some tunnel activity occurred.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: The unique identifier for the tunnel, which may correspond to a connection’s uid
       * field for non-IP-in-IP tunnels. This is optional because there could be numerous
       * connections for payload proxies like SOCKS but we should treat it as a single tunnel.
       */
      StructField("uid", StringType, nullable = true)

    )
    /* id: The tunnel “connection” 4-tuple of endpoint addresses/ports. For an IP tunnel,
     * the ports will be 0.
     */
    fields = fields ++ conn_id()

    fields = fields ++ Array(

      /* tunnel_type: Time at which some tunnel activity occurred.
       */
      StructField("tunnel_type",  StringType, nullable = false),

      /* action: The type of activity that occurred.
       */
      StructField("action", StringType, nullable = false)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * weird (&log)
   *
   * {
   * 	"ts":1543877999.99354,
   * 	"uid":"C1ralPp062bkwWt4e",
   * 	"id.orig_h":"192.168.1.1",
   * 	"id.orig_p":64521,
   * 	"id.resp_h":"192.168.1.2",
   * 	"id.resp_p":53,
   * 	"name":"dns_unmatched_reply",
   * 	"notice":false,
   * 	"peer":"worker-6"
   * }
   *
   * This log provides a default set of actions to take for “weird activity” events generated
   * from Zeek’s event engine. Weird activity is defined as unusual or exceptional activity that
   * can indicate malformed connections, traffic that doesn’t conform to a particular protocol,
   * malfunctioning or misconfigured hardware, or even an attacker attempting to avoid/confuse
   * a sensor.
   *
   * Without context, it’s hard to judge whether a particular category of weird activity is interesting,
   * but this script provides a starting point for the user.
   */
  def fromWeird(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromWeird(log.getAsJsonObject, schema)
    })
  }

  def fromWeird(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceConnId(newObject)

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def weird():StructType = {

    var fields = Array(

      /* ts: Timestamp for when the weird occurred.
       */
      StructField("ts", LongType, nullable = false),

      /* uid: If a connection is associated with this weird, this will be the
       * connection’s unique ID.
       */
      StructField("uid", StringType, nullable = true)

    )
    /* id
     */
    fields = fields ++ conn_id_nullable()

    fields = fields ++ Array(

      /* name: The name of the weird that occurred.
       */
      StructField("name", StringType, nullable = false),

      /* addl: Additional information accompanying the weird if any.
       */
      StructField("addl", StringType, nullable = true),

      /* notice: Indicate if this weird was also turned into a notice.
       */
      StructField("notice", BooleanType, nullable = true),

      /* peer: The peer that originated this weird. This is helpful in cluster
       * deployments if a particular cluster node is having trouble to help identify
       * which node is having trouble.
       */
      StructField("peer", StringType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

  }
  /**
   * x509 (&log)
   *
   * {
   * 	"ts":1543867200.143484,
   * 	"id":"FxZ6gZ3YR6vFlIocq3",
   * 	"certificate.version":3,
   * 	"certificate.serial":"2D00003299D7071DB7D1708A42000000003299",
   * 	"certificate.subject":"CN=www.bing.com",
   * 	"certificate.issuer":"CN=Microsoft IT TLS CA 5,OU=Microsoft IT,O=Microsoft Corporation,L=Redmond,ST=Washington,C=US",
   * 	"certificate.not_valid_before":1500572828.0,
   * 	"certificate.not_valid_after":1562780828.0,
   * 	"certificate.key_alg":"rsaEncryption",
   * 	"certificate.sig_alg":"sha256WithRSAEncryption",
   * 	"certificate.key_type":"rsa",
   * 	"certificate.key_length":2048,
   * 	"certificate.exponent":"65537",
   * 	"san.dns":["www.bing.com","dict.bing.com.cn","*.platform.bing.com","*.bing.com","bing.com","ieonline.microsoft.com","*.windowssearch.com","cn.ieonline.microsoft.com","*.origin.bing.com","*.mm.bing.net","*.api.bing.com","ecn.dev.virtualearth.net","*.cn.bing.net","*.cn.bing.com","ssl-api.bing.com","ssl-api.bing.net","*.api.bing.net","*.bingapis.com","bingsandbox.com","feedback.microsoft.com","insertmedia.bing.office.net","r.bat.bing.com","*.r.bat.bing.com","*.dict.bing.com.cn","*.dict.bing.com","*.ssl.bing.com","*.appex.bing.com","*.platform.cn.bing.com","wp.m.bing.com","*.m.bing.com","global.bing.com","windowssearch.com","search.msn.com","*.bingsandbox.com","*.api.tiles.ditu.live.com","*.ditu.live.com","*.t0.tiles.ditu.live.com","*.t1.tiles.ditu.live.com","*.t2.tiles.ditu.live.com","*.t3.tiles.ditu.live.com","*.tiles.ditu.live.com","3d.live.com","api.search.live.com","beta.search.live.com","cnweb.search.live.com","dev.live.com","ditu.live.com","farecast.live.com","image.live.com","images.live.com","local.live.com.au","localsearch.live.com","ls4d.search.live.com","mail.live.com","mapindia.live.com","local.live.com","maps.live.com","maps.live.com.au","mindia.live.com","news.live.com","origin.cnweb.search.live.com","preview.local.live.com","search.live.com","test.maps.live.com","video.live.com","videos.live.com","virtualearth.live.com","wap.live.com","webmaster.live.com","webmasters.live.com","www.local.live.com.au","www.maps.live.com.au"]
   * }
   *
   */
  def fromX509(logs:Seq[JsonElement], schema:StructType):Seq[Row] = {
    logs.map(log => {
      fromX509(log.getAsJsonObject, schema)
    })
  }

  def fromX509(oldObject:JsonObject, schema:StructType):Row = {

    var newObject = oldObject
    /*
     * Prepare JsonObject, i.e. rename fields and
     * transform time values
     */
    newObject = replaceTime(newObject, "ts")
    newObject = replaceCertificate(newObject)

    newObject = replaceName(newObject, "san_dns", "san.dns")
    newObject = replaceName(newObject, "san_uri", "san.uri")

    newObject = replaceName(newObject, "san_email", "san.email")
    newObject = replaceName(newObject, "san_ip", "san.ip")

    newObject = replaceName(newObject, "san_other_fields", "san.other_fields")

    newObject = replaceName(newObject, "basic_constraints_ca", "basic_constraints.ca")
    newObject = replaceName(newObject, "basic_constraints_path_len", "basic_constraints.path_len")

    /* Transform into row */
    JsonUtil.json2Row(newObject, schema)

  }

  def x509():StructType = {

    var fields = Array(

      /* ts: Current timestamp.
       */
      StructField("ts", LongType, nullable = false),

      /* id: File id of this certificate.
       */
      StructField("id", StringType, nullable = true)

    )

    /*** CERTIFICATE DESCRIPTION ***/

    /* certificate: Basic information about the certificate.
     */
    fields = fields ++ certificate()

    fields = fields ++ Array(

      /* san.dns: List of DNS entries in SAN (Subject Alternative Name)
       */
      StructField("san_dns", ArrayType(StringType), nullable = true),

      /* san.uri: List of URI entries in SAN
       */
      StructField("san_uri", ArrayType(StringType), nullable = true),

      /* san.email: List of email entries in SAN
       */
      StructField("san_email", ArrayType(StringType), nullable = true),

      /* san.ip: List of IP entries in SAN
       */
      StructField("san_ip", ArrayType(StringType), nullable = true),

      /* san.other_fields: True if the certificate contained other, not recognized or parsed name fields.
       */
      StructField("san_other_fields", BooleanType, nullable = true),

      /*** BASIC CONSTRAINTS ***/

      /* basic_constraints.ca: CA flag set?
       */
      StructField("basic_constraints_ca", BooleanType, nullable = true),

      /* basic_constraints.path_len: Maximum path length.
       */
      StructField("basic_constraints_path_len", IntegerType, nullable = true)

    )

    fields = Array(primaryKey) ++ fields
    StructType(fields)

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

  private def conn_id_nullable():Array[StructField] = {

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
