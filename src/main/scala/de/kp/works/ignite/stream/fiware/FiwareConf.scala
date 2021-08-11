package de.kp.works.ignite.stream.fiware
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

object FiwareConf {

  private val path = "reference.conf"
  /*
   * The following parameters configure the Fiware
   * notification server and must adapted to the
   * current environment
   */
  private var systemName = "fiware-server"

  private var brokerUrl = ""

  private var httpHost = "127.0.0.1"
  private var httpPort = 9090


  /**
   * This is the reference to the overall configuration
   * file that holds all configuration required for this
   * application
   */
  private var cfg:Option[Config] = None

  def init(config:Option[String] = None):Boolean = {

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
        extractCfg()
        true

      } catch {
        case t:Throwable =>
          false
      }
    }
  }

  def isInit:Boolean = {
    cfg.isDefined
  }

  private def extractCfg():Unit = {

    val fiwareCfg = cfg.get.getConfig("fiware")

    /* BROKER */

    val broker = fiwareCfg.getConfig("broker")
    brokerUrl = broker.getString("endpoint")

    /* NOTIFICATION SERVER */

    val binding = fiwareCfg.getConfig("binding")

    httpHost = binding.getString("host")
    httpPort = binding.getInt("port")

  }
  /**
   * The configuration of the Fiware (notification)
   * actor, which is responsible for retrieving and
   * executing Orion Broker notification requests
   */
  def getActorCfg:Config = {

    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("actor")

  }
  /**
   * Retrieve the SSL/TLS configuration for subscription
   * requests to the Orion Context Broker
   */
  def getBrokerSecurity:Config = {
    val security = cfg.get.getConfig("security")
    security.getConfig("fiware")
  }

  def getBrokerUrl: String = brokerUrl

  def getDataModel:Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("model")
  }
  /**
   * Retrieve the SSL/TLS configuration for subscription
   * requests to the Orion Context Broker
   */
  def getFiwareSecurity:Config = {
    val security = cfg.get.getConfig("security")
    security.getConfig("fiware")
  }

  def getStreamerCfg:Config = {
    val fiwareCfg = cfg.get.getConfig("fiware")
    fiwareCfg.getConfig("streamer")
  }
  /**
   * The host & port configuration of the HTTP server that
   * is used as a notification endpoint for an Orion Context
   * Broker instance
   */
  def getServerBinding: (String, Int) = (httpHost, httpPort)

  /**
   * Retrieve the SSL/TLS configuration for notification
   * requests from the Orion Context Broker
   */
  def getServerSecurity:Config = {
    val security = cfg.get.getConfig("security")
    security.getConfig("server")
  }
  /**
   * The name of Actor System used
   */
  def getSystemName: String = systemName

  /**
   * Properties:
   *
   * - timeWindow
   * - ignite.autoFlushFrequency
   * - ignite.numThreads
   */

}
