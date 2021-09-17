package de.kp.works.conf
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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.logger.java.JavaLogger

object WorksConf {

  private val path = "reference.conf"

  val FIWARE_CONF   = "fiware"
  val FLEETDM_CONF  = "osquery_fleet"
  val OPENCTI_CONF  = "opencti"
  val OSQUERY_CONF  = "osquery_tls"
  val ZEEK_CONF     = "zeek"

  /**
   * This is the reference to the overall configuration
   * file that holds all configuration required for this
   * application
   */
  private var cfg: Option[Config] = None

  def init(config: Option[String] = None): Boolean = {

    if (cfg.isDefined) true
    else {
      try {

        cfg = if (config.isDefined) {
          /*
           * An external configuration file is provided
           * and must be transformed into a Config
           */
          Option(ConfigFactory.parseString(config.get))

        } else {
          /*
           * The internal reference file is used to
           * extract the required configurations
           */
          Option(ConfigFactory.load(path))

        }
        true

      } catch {
        case t: Throwable =>
          false
      }
    }
  }

  def isInit: Boolean = {
    cfg.isDefined
  }

  /** COMMON CONFIGURATION **/

  def getCfg(name:String):Config = {
    name match {
      case FIWARE_CONF =>
        cfg.get.getConfig("fiware")
      case FLEETDM_CONF =>
        cfg.get.getConfig("osquery_fleet")
      case OPENCTI_CONF =>
        cfg.get.getConfig("opencti")
      case OSQUERY_CONF =>
        cfg.get.getConfig("osquery_tls")
      case ZEEK_CONF =>
        cfg.get.getConfig("zeek")
      case _ =>
        throw new Exception(s"Configuration for `$name` is not supported.")
    }
  }
  /**
   * The current version of this project supports four different
   * data sources, Fiware, FleetDM, OpenCTI, Osquery and Zeek.
   * This choice is based on our Cy(I)IoT initiative to bring
   * endpoints, network and data to a single platform.
   */
  def getNSCfg(name:String):String = {
    val conf = getCfg(name)
    conf.getString("namespace")
  }
  /**
   * This method offers the configuration for those
   * Apache Ignite streamers, that are based on a
   * Receiver
   */
  def getReceiverCfg(name:String):Config = {

    if (Array(
      FLEETDM_CONF,
      OPENCTI_CONF,
      ZEEK_CONF).contains(name)) {

      val conf = getCfg(name)
      conf.getConfig("receiver")

    }
    else
       throw new Exception(s"Receiver configuration for `$name` is not supported.")

  }
  /**
   * This method offers the configuration for those
   * Apache Ignite streamers, that are based on a
   * HTTP(s) server
   */
  def getServerCfg(name:String):Config = {

    if(Array(
      FIWARE_CONF,
      OSQUERY_CONF).contains(name)) {

      val conf = getCfg(name)
      conf.getConfig("server")

    }
    else
      throw new Exception(s"Server configuration for `$name` is not supported.")

  }
  /**
   * This method offers the configuration for the
   * Apache Ignite streamer, that is at the heart
   * of every data streaming support.
   */
  def getStreamerCfg(name:String): Config = {
    val conf = getCfg(name)
    conf.getConfig("streamer")
  }
  /**
   * The name of Actor System used: Fiware and Osquery
   * uses an Actor system as foundation of the HTTP(s)
   * server, while Zeek leverages an Actor system as
   * basis for its file monitor.
   */
  def getSystemName(name:String): String = {

    name match {
      case FLEETDM_CONF => "fleetdm-monitor"
      case FIWARE_CONF  => "fiware-server"
      case OSQUERY_CONF => "osquery-server"
      case ZEEK_CONF    => "zeek-monitor"
      case _ =>
        throw new Exception(s"Actor system for `$name` is not supported.")
    }

  }

  /** FIWARE CONFIGURATION **/

  /**
   * The configuration of the Fiware (notification)
   * actor, which is responsible for retrieving and
   * executing Orion Broker notification requests
   */
  def getFiwareActorCfg: Config = {

    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("actor")

  }
  /**
   * Retrieve the SSL/TLS configuration for subscription
   * requests to the Orion Context Broker
   */
  def getFiwareBrokerSecurity: Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    val brokerCfg = fiwareCfg.getConfig("broker")

    brokerCfg.getConfig("security")
  }

  def getFiwareBrokerUrl: String = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    val brokerCfg = fiwareCfg.getConfig("broker")

    brokerCfg.getString("endpoint")

  }

  def getFiwareDataModel: Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("model")
  }

  /** IGNITE CONFIGURATION **/

  def getIgniteCfg: Config = {
    cfg.get.getConfig("ignite")
  }

  /** SPARK CONFIGURATION **/

  def getSparkCfg: Config = {
    cfg.get.getConfig("spark")
  }

  /**
   * The current implementation of this method
   * provides the default configuration
   */
  def getIgniteConfiguration: IgniteConfiguration = {
    /*
     * Configure default java logger which leverages file
     * config/java.util.logging.properties
     */
    val logger = new JavaLogger()
    /*
     * The current Ignite context is configured with the
     * default configuration (except 'marshaller')
     */
    val igniteCfg = new IgniteConfiguration()
    igniteCfg.setGridLogger(logger)

    // TODO Customize with project specific settings
    igniteCfg
  }

}
