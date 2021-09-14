package de.kp.works.ignite.stream.opencti

import de.kp.works.conf.WorksConf
import de.kp.works.ignite.stream.BaseStream

/**
 * [CTIStream] is the OpenCTI streaming application
 * of [IgniteGraph]
 */
object CTIStream extends BaseStream {

  override var channel: String = WorksConf.OPENCTI_CONF

  override var programName: String = "CTIStream"
  override var programDesc: String = "Ignite streaming support for threat intel events."

  override def launch(args: Array[String]): Unit = {

    /* Command line argument parser */
    val parser = buildParser()

    /* Parse the argument and then run */
    parser.parse(args, CliConfig()).foreach { c =>

      try {

        connect = Some(buildConnect(c, channel))
        /*
         * Build streaming context and finally start the
         * service that listens to OpenCTI events.
         */
        val ctiIgnite = new CTIEngine(connect.get)
        service = ctiIgnite.buildStream

        start()

        println("[INFO] -------------------------------------------------")
        println(s"[INFO] $programName started.")
        println("[INFO] -------------------------------------------------")

      } catch {
        case t: Throwable =>
          t.printStackTrace()
          println("[ERROR] --------------------------------------------------")
          println(s"[ERROR] $programName cannot be started: " + t.getMessage)
          println("[ERROR] --------------------------------------------------")
          /*
           * Sleep for 10 seconds so that one may see error messages
           * in Yarn clusters where logs are not stored.
           */
          Thread.sleep(10000)
          sys.exit(1)

      }
    }

  }
}
