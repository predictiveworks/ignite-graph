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

import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.fiware.{FiwareConf, FiwareIgnite}
import scopt.OptionParser

object FiwareStream {

  private case class CliConfig(
    /*
     * The command line interface supports the provisioning
     * of a typesafe config compliant configuration file
     */
    conf:String = null
  )

  private var connect:Option[IgniteConnect] = None
  private var service:Option[IgniteStreamContext] = None

  def main(args:Array[String]):Unit = {
    launch(args)
  }

  def launch(args:Array[String]):Unit = {

    /* Command line argument parser */
    val parser = new OptionParser[CliConfig]("FiwareStream") {

      head("Fiware Stream: Streaming support for Fiware notifications.")
      opt[String]("c")
        .text("The path to the configuration file.")
        .action((x, c) => c.copy(conf = x))

    }
    /* Parse the argument and then run */
    parser.parse(args, CliConfig()).map{c =>

      try {

        if (c.conf == null) {

          println("[INFO] -------------------------------------------------")
          println("[INFO] Launch Fiware Stream with internal configuration.")
          println("[INFO] -------------------------------------------------")

          FiwareConf.init()

        } else {

          println("[INFO] -------------------------------------------------")
          println("[INFO] Launch Fiware Stream with external configuration.")
          println("[INFO] -------------------------------------------------")

          val source = scala.io.Source.fromFile(c.conf)
          val config = source.getLines.mkString("\n")

          FiwareConf.init(Option(config))
          source.close()

        }

        /*
         * Initialize connection to Apache Ignite
         */

        // TODO

        start()

        println("[INFO] -------------------------------------------------")
        println("[INFO] Fiware Stream started.")
        println("[INFO] -------------------------------------------------")

      } catch {
        case t:Throwable =>
          t.printStackTrace()
          println("[ERROR] -------------------------------------------------")
          println("[ERROR] Fiware Stream cannot be started: " + t.getMessage)
          println("[ERROR] -------------------------------------------------")
      }
    }.getOrElse {
      /*
       * Sleep for 10 seconds so that one may see error messages
       * in Yarn clusters where logs are not stored.
       */
      Thread.sleep(10000)
      sys.exit(1)
    }

  }

  def start():Unit = {
    /*
     * Build streaming context
     */
    val fiwareIgnite = new FiwareIgnite(connect.get)
    service = fiwareIgnite.buildStream

    if (service.isEmpty)
      throw new Exception("Initialization of the Fiware Streamer failed.")

    service.get.start()

  }

  def stop():Unit = {
    if (service.isDefined)
      service.get.stop()
  }

}