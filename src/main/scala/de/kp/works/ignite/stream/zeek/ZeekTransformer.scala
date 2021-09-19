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

import com.google.gson.JsonParser
import de.kp.works.ignite.stream.zeek.ZeekFormats._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * The [ZeekTransformer] transforms 35+ Zeek log events
 * into Apache Spark schema-compliant Rows
 */
object ZeekTransformer {

  def transform(events:Seq[ZeekEvent]):Seq[(ZeekFormat, StructType, Seq[Row])] = {

    try {
      /*
       * STEP #1: Collect Zeek log events with respect
       * to their eventType (which refers to the name
       * of the log file)
       */
      val data = events
        /*
         * Convert `eventType` (file name) into Zeek format
         * and deserialize event data. Restrict to those
         * formats that are support by the current version.
         */
        .map(event =>
          (ZeekFormatUtil.fromFile(event.eventType), JsonParser.parseString(event.eventData))
        )
        .filter{case(format, _) => format != null}
        /*
         * Group logs by format and prepare format specific
         * log processing
         */
        .groupBy{case(format, _) => format}
        .map{case(format, logs) => (format, logs.map(_._2))}
        .toSeq
      /*
       * STEP #2: Persist logs for each format individually
       */
      data.map{case(format, logs) =>

        format match {
          case CAPTURE_LOSS =>
            val schema = ZeekUtil.capture_loss()
            val rows = ZeekUtil.fromCaptureLoss(logs, schema)

            (format, schema, rows)

          case CONNECTION =>
            val schema = ZeekUtil.connection()
            val rows = ZeekUtil.fromConnection(logs, schema)

            (format, schema, rows)

         case DCE_RPC =>
           val schema = ZeekUtil.dce_rpc()
           val rows = ZeekUtil.fromDceRpc(logs, schema)

           (format, schema, rows)

          case DHCP =>
            val schema = ZeekUtil.dhcp()
            val rows = ZeekUtil.fromDhcp(logs, schema)

            (format, schema, rows)

          case DNP3 =>
            val schema = ZeekUtil.dnp3()
            val rows = ZeekUtil.fromDnp3(logs, schema)

            (format, schema, rows)

          case DNS =>
            val schema = ZeekUtil.dns()
            val rows = ZeekUtil.fromDns(logs, schema)

            (format, schema, rows)

          case DPD =>
            val schema = ZeekUtil.dpd()
            val rows = ZeekUtil.fromDpd(logs, schema)

            (format, schema, rows)

          case FILES =>
            val schema = ZeekUtil.files()
            val rows = ZeekUtil.fromFiles(logs, schema)

            (format, schema, rows)

          case FTP =>
            val schema = ZeekUtil.ftp()
            val rows = ZeekUtil.fromFtp(logs, schema)

            (format, schema, rows)

          case HTTP =>
            val schema = ZeekUtil.http()
            val rows = ZeekUtil.fromHttp(logs, schema)

            (format, schema, rows)

//          case INTEL =>
//          case IRC =>
//          case KERBEROS =>
//          case MODBUS =>
//          case MYSQL =>
//          case NOTICE =>
//          case NTLM =>
//          case OCSP =>
//          case PE =>
//          case RADIUS =>
//          case RDP =>
//          case RFB =>
//          case SIP =>
//          case SMB_CMD =>
//          case SMB_FILES =>
//          case SMB_MAPPING =>
//          case SMTP =>
//          case SNMP =>
//          case SOCKS =>
//          case SSH =>
//          case SSL =>
//          case STATS =>
//          case SYSLOG =>
//          case TRACEROUTE =>
//          case TUNNEL =>
//          case WEIRD =>
//          case X509 =>
          case _ => throw new Exception(s"[ZeekTransformer] Unknown format `$format.toString` detected.")

        }
      }

    } catch {
      case _:Throwable => Seq.empty[(ZeekFormat, StructType, Seq[Row])]
    }

  }

}
