package de.kp.works.ignite.stream.osquery.tls

import de.kp.works.ignite.client.IgniteConnect
import de.kp.works.ignite.stream.osquery.{OsqueryEvent, OsqueryWriter}

class TLSWriter(connect:IgniteConnect) extends OsqueryWriter(connect) {

  override def write(events: Seq[OsqueryEvent]): Unit = ???

}
