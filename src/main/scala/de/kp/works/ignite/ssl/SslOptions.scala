package de.kp.works.ignite.ssl
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
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security._
import java.security.cert.X509Certificate
import javax.net.ssl._

class AllTrustManager extends X509TrustManager {

  def getAcceptedIssuers:Array[X509Certificate] = {
    Array.empty[X509Certificate]
  }

  def checkClientTrusted(chain:Array[X509Certificate], authType:String):Unit = {
  }

  def checkServerTrusted(chain:Array[X509Certificate], authType:String):Unit = {
  }

}

class SslOptions(
    /* KEY STORE */

    /* Path to the keystore file */
    ksFile: Option[String] = None,
    /* Keystore type */
    ksType: Option[String] = None,
    /* Keystore password */
    ksPass: Option[String] = None,
    /* Keystore algorithm */
    ksAlgo: Option[String] = None,

    /* TRUST STORE */

    /* Path to the truststore file */
    tsFile: Option[String] = None,
    /* Truststore type */
    tsType: Option[String] = None,
    /* Truststore password */
    tsPass: Option[String] = None,
    /* Truststore algorithm */
    tsAlgo: Option[String] = None,

    /* CERTIFICATES FILES */

    caCertFile: Option[String] = None,
    certFile: Option[String] = None,
    privateKeyFile: Option[String] = None,
    privateKeyFilePass: Option[String] = None) {

  private val TLS_VERSION = "TLS"

  def getSSLContext: SSLContext = {

    var keyManagers:Array[KeyManager] = null
    var trustManagers:Array[TrustManager] = null

    /** KEY STORE **/

    if (ksFile.isDefined && ksType.isDefined && ksPass.isDefined && ksAlgo.isDefined) {

      keyManagers = SslUtil.getStoreKeyManagers(
        ksFile.get,  ksType.get, ksPass.get, ksAlgo.get)

    }

    /** TRUST STORE **/

    if (tsFile.isDefined && tsType.isDefined & tsPass.isDefined && tsAlgo.isDefined) {

      trustManagers = SslUtil.getStoreTrustManagers(
        tsFile.get, tsType.get,  tsPass.get, tsAlgo.get)

    }


    /** CERTIFICATE FILE & PRIVATE KEY **/

    Security.addProvider(new BouncyCastleProvider())

    if (certFile.isDefined && privateKeyFile.isDefined && privateKeyFilePass.isDefined) {
      /*
       * SSL authentication based on a provided client certificate file,
       * private key file and associated password; the certificate will
       * be added to a newly created key store
       */

      keyManagers = SslUtil.getCertFileKeyManagers(
        certFile.get, privateKeyFile.get, privateKeyFilePass.get)

    }

    if (caCertFile.isDefined)
      trustManagers = SslUtil.getCertFileTrustManagers(caCertFile.get)


    val secureRandom = Option(new SecureRandom())
    buildSSLContext(keyManagers, trustManagers, secureRandom)

  }

  def getTrustAllContext: SSLContext = {

    val keyManagers:Array[KeyManager] = null
    val trustManagers:Array[TrustManager] = Array(new AllTrustManager())

    val secureRandom = Option(new SecureRandom())
    buildSSLContext(keyManagers, trustManagers, secureRandom)

  }

  private def buildSSLContext(keyManagers:Seq[KeyManager], trustManagers:Seq[TrustManager], secureRandom:Option[SecureRandom]) = {

    val sslContext = SSLContext.getInstance(TLS_VERSION)

    sslContext.init(nullIfEmpty(keyManagers.toArray), nullIfEmpty(trustManagers.toArray), secureRandom.orNull)
    sslContext

  }

  private def nullIfEmpty[T](array: Array[T]) = {
    if (array.isEmpty) null else array
  }

}