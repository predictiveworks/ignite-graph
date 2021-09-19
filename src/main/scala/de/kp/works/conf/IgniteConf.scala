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

import org.apache.ignite.configuration.{DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.logger.java.JavaLogger

/**
 * Apache Ignite Spark ships with two different
 * approaches to access configuration information:
 *
 * When starting an Apache Ignite node, an instance
 * of [IgniteConfiguration] is required, while Ignite
 * dataframes requires the path to a configuration file.
 */
object IgniteConf {
  /*
   * This implementation expects that the configuration
   * file is located resources/META-INF as in this case,
   * Apache Ignite automatically detects files in this
   * folder
   */
  val file = "ignite-config.xml"
  /*
   * Configure default java logger which leverages file
   * config/java.util.logging.properties
   */
  val logger = new JavaLogger()
  /*
   * The current Ignite context is configured with the
   * default configuration (except 'marshaller')
   */
  val config = new IgniteConfiguration()
  config.setGridLogger(logger)

  val ds = new DataStorageConfiguration()
  /*
   * Lessons learned: at least 750 MB
   */
  ds.setSystemRegionMaxSize(2L * 1024 * 1024 * 1024)
  config.setDataStorageConfiguration(ds)

  def fromFile:String = file

  def fromConfig:IgniteConfiguration = config

}
