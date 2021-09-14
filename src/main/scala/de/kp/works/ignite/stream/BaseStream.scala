package de.kp.works.ignite.stream
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

import de.kp.works.conf.WorksConf
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.spark.Session

import scopt.OptionParser

trait BaseStream {

  protected case class CliConfig(
    /*
     * The command line interface supports the provisioning
     * of a typesafe config compliant configuration file
     */
    conf: String = null
  )

  protected var programName:String
  protected var programDesc:String

  protected var channel:String

  protected var connect: Option[IgniteConnect] = None
  protected var service: Option[IgniteStreamContext] = None

  private val fileHelpText = "The path to the configuration file."

  protected def buildParser():OptionParser[CliConfig] = {

    val parser = new OptionParser[CliConfig](programName) {

      head(programDesc)
      opt[String]("c")
        .text(fileHelpText)
        .action((x, c) => c.copy(conf = x))
    }

    parser

  }

  protected def buildConnect(c:CliConfig, channel:String):IgniteConnect = {
    /*
     * STEP #1: Initialize the common configuration
     * either from an internal or external config
     */
    val cfg = buildConfig(c)
    WorksConf.init(cfg)
    /*
     * STEP #2: Initialize connection to Apache Ignite
     */
    val namespace = WorksConf.getNSCfg(channel)
    val igniteCfg = WorksConf.getIgniteConfiguration

    IgniteConnect.getInstance(Session.getSession, igniteCfg, namespace)

  }

  private def buildConfig(c:CliConfig):Option[String] = {

    if (c.conf == null) {

      println("[INFO] -------------------------------------------------")
      println(s"[INFO] Launch $programName with internal configuration.")
      println("[INFO] -------------------------------------------------")

      None

    } else {

      println("[INFO] -------------------------------------------------")
      println(s"[INFO] Launch $programName with external configuration.")
      println("[INFO] -------------------------------------------------")

      val source = scala.io.Source.fromFile(c.conf)
      val config = source.getLines.mkString("\n")

      source.close()
      Some(config)

    }

  }
  def main(args: Array[String]): Unit = {
    launch(args)
  }

  def launch(args: Array[String]): Unit

  def start(): Unit = {

    if (service.isEmpty)
      throw new Exception("Initialization of the Ignite streaming service failed.")

    service.get.start()

  }

  def stop(): Unit = {
    if (service.isDefined)
      service.get.stop()
  }

}
