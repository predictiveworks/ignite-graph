package de.kp.works.ignite.streamer.opencti.table
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.{Logger, LoggerFactory}

import java.util.{Date, UUID}
import scala.collection.mutable

trait BaseUtil {

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[BaseUtil])

  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  protected def transformHashes(hashes: Any): Map[String, String] = {

    val result = mutable.HashMap.empty[String, String]
    /*
     * Flatten hashes
     */
    hashes match {
      case _: List[Any] =>
        hashes.asInstanceOf[List[Map[String, String]]].foreach(hash => {
          val k = hash("algorithm")
          val v = hash("hash")

          result += k -> v
        })
      case _ =>
        try {
          hashes.asInstanceOf[Map[String,String]].foreach(entry => {
            result += entry._1 -> entry._2.asInstanceOf[String]
          })

        } catch {
          case _:Throwable =>
            val now = new Date().toString
            throw new Exception(s"[ERROR] $now - Unknown data type for hashes detected.")
        }
    }
    result.toMap

  }

  protected def getBasicType(attrVal: Any): String = {
    attrVal match {
      /*
       * Basic data types: these data type descriptions
       * are harmonized with [ValueType]
       */
      case _: BigDecimal => "DECIMAL"
      case _: Boolean => "BOOLEAN"
      case _: Byte => "BYTE"
      case _: Double => "DOUBLE"
      case _: Float => "FLOAT"
      case _: Int => "INT"
      case _: Long => "LONG"
      case _: Short => "SHORT"
      case _: String => "STRING"
      /*
       * Datetime support
       */
      case _: java.sql.Date => "DATE"
      case _: java.sql.Timestamp => "TIMESTAMP"
      case _: java.util.Date => "DATE"
      case _: java.time.LocalDate => "DATE"
      case _: java.time.LocalDateTime => "DATE"
      case _: java.time.LocalTime => "TIMESTAMP"
      /*
       * Handpicked data types
       */
      case _: UUID => "UUID"
      case _ =>
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Basic data type not supported.")
    }

  }

}
