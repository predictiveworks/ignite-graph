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

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import com.typesafe.config.Config
import de.kp.works.conf.CommonConfig
import de.kp.works.ignite.ssl.SslOptions

import javax.net.ssl.SSLContext

object FiwareSsl {

  def isFiwareSsl: Boolean = {
    /*
      * Distinguish between SSL/TLS and non-SSL/TLS requests;
      * note, [IgniteConf] must be initialized.
      */
    val cfg = CommonConfig.getFiwareBrokerSecurity
    if (cfg.getString("ssl") == "false") false else true
  }

  def isServerSsl: Boolean = {
    /*
      * Distinguish between SSL/TLS and non-SSL/TLS requests;
      * note, [IgniteConf] must be initialized.
      */
    val cfg = CommonConfig.getFiwareServerSecurity
    if (cfg.getString("ssl") == "false") false else true
  }

  def buildBrokerContext: HttpsConnectionContext = {
    val cfg = CommonConfig.getFiwareBrokerSecurity
    ConnectionContext.https(buildSSLContext(cfg))
  }

  def buildServerContext: HttpsConnectionContext = {
    val cfg = CommonConfig.getFiwareServerSecurity
    ConnectionContext.https(buildSSLContext(cfg))
  }

  private def buildSSLContext(securityCfg: Config): SSLContext = {

    val sslOptions = getSslOptions(securityCfg)
    sslOptions.getSslContext

  }

  private def getSslOptions(securityCfg: Config): SslOptions = {
    SslOptions.getOptions(securityCfg)
  }

}
