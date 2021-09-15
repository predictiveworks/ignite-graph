package de.kp.works.ignite.stream.zeek.actor
/*
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.{Actor, ActorLogging}
import de.kp.works.ignite.stream.zeek.ZeekEventHandler

import java.nio.file.Path

object FileActor {

  sealed trait FileEvent

  case class Created() extends FileEvent
  case class Deleted() extends FileEvent
  case class Modified() extends FileEvent

}

class FileActor(path:Path, eventHandler: ZeekEventHandler) extends Actor with ActorLogging {

  import FileActor._

  override def receive: Receive = {
    case event:Created =>
    case event:Deleted =>
    case event:Modified =>
    case _ =>
      throw new Exception(s"Unknown file event detected")
  }

}
