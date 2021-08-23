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

object CommonConfig {

  private val path = "reference.conf"
  /*
   * The following parameters configure the Fiware
   * notification server and must adapted to the
   * current environment
   */
  private val systemName = "fiware-server"

  private var brokerUrl = ""

  private var httpHost = "127.0.0.1"
  private var httpPort = 9090


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

  def getFiwareGraphNS: String = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getString("namespace")
  }

  /**
   * The host & port configuration of the HTTP server that
   * is used as a notification endpoint for an Orion Context
   * Broker instance
   */
  def getFiwareServerBinding: Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    val serverCfg = fiwareCfg.getConfig("server")

    serverCfg.getConfig("binding")
  }

  /**
   * Retrieve the SSL/TLS configuration for notification
   * requests from the Orion Context Broker
   */
  def getFiwareServerSecurity: Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    val serverCfg = fiwareCfg.getConfig("server")

    serverCfg.getConfig("security")
  }

  def getFiwareStreamerCfg: Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("streamer")
  }

  /** IGNITE CONFIGURATION **/

  def getIgniteCfg: Config = {
    cfg.get.getConfig("ignite")
  }

  /** OPENCTI CONFIGURATION **/

  def getCTIStreamerCfg: Config = {
    val ctiCfg = cfg.get.getConfig("opencti")
    ctiCfg.getConfig("streamer")
  }

  /** SPARK CONFIGURATION **/

  def getSparkCfg: Config = {
    cfg.get.getConfig("spark")
  }

  /**
   * The name of Actor System used
   */
  def getSystemName: String = systemName

  /**
   * The current implementation of this method
   * provides the default configuration
   */
  def toIgniteConfiguration: IgniteConfiguration = {
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
