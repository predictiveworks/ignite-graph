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

import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.fiware.FiwareEngine
import de.kp.works.spark.Session
import scopt.OptionParser
/**
 * [FiwareStream] is the FIWARE streaming application
 * of [IgniteGraph]
 */

object FiwareStream extends BaseStream {

  override var programName: String = "FiwareStream"
  override var programDesc: String = "Ignite streaming support for Fiware notifications."

  override def launch(args:Array[String]):Unit = {

    /* Command line argument parser */
    val parser = buildParser()

    /* Parse the argument and then run */
    parser.parse(args, CliConfig()).map{c =>

      try {

        if (c.conf == null) {

          println("[INFO] -------------------------------------------------")
          println("[INFO] Launch Fiware Stream with internal configuration.")
          println("[INFO] -------------------------------------------------")

          CommonConfig.init()

        } else {

          println("[INFO] -------------------------------------------------")
          println("[INFO] Launch Fiware Stream with external configuration.")
          println("[INFO] -------------------------------------------------")

          val source = scala.io.Source.fromFile(c.conf)
          val config = source.getLines.mkString("\n")

          CommonConfig.init(Option(config))
          source.close()

        }

        /*
         * Initialize connection to Apache Ignite
         */
        connect = Some(IgniteConnect.getInstance(
          Session.getSession,
          CommonConfig.toIgniteConfiguration,
          CommonConfig.getFiwareGraphNS))

        /*
         * Build streaming context and finally start the
         * service that listens to Fiware events.
         */
        val fiwareIgnite = new FiwareEngine(connect.get)
        service = fiwareIgnite.buildStream

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

}