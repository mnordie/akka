/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.remote.transport.netty

import java.io.{ FileNotFoundException, IOException }
import java.nio.file.{ Files, Paths }
import java.security.{ KeyStore, _ }
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl._

import akka.actor.Address
import akka.event.{ LogMarker, MarkerLoggingAdapter }
import akka.japi.Util._
import akka.remote.RemoteTransportException
import akka.remote.security.provider.AkkaProvider
import com.typesafe.config.Config
import org.jboss.netty.handler.ssl.SslHandler

import scala.annotation.tailrec
import scala.util.Try

/**
 * INTERNAL API
 */
private[akka] class SSLSettings(config: Config) {
  import config.{ getBoolean, getString, getStringList }

  val SSLKeyStore = getString("key-store")
  val SSLTrustStore = getString("trust-store")
  val SSLKeyStorePassword = getString("key-store-password")
  val SSLKeyPassword = getString("key-password")

  val SSLTrustStorePassword = getString("trust-store-password")

  val SSLEnabledAlgorithms = immutableSeq(getStringList("enabled-algorithms")).to[Set]

  val SSLProtocol = getString("protocol")

  val SSLRandomNumberGenerator = getString("random-number-generator")

  val SSLRequireMutualAuthentication = getBoolean("require-mutual-authentication")

  val SSLRequireHostnameValidation = getBoolean("require-hostname-validation")

  private val sslContext = new AtomicReference[SSLContext]()
  @tailrec final def getOrCreateContext(log: MarkerLoggingAdapter): SSLContext =
    sslContext.get() match {
      case null ⇒
        val newCtx = constructContext(log)
        if (sslContext.compareAndSet(null, newCtx)) newCtx
        else getOrCreateContext(log)
      case ctx ⇒ ctx
    }

  private def constructContext(log: MarkerLoggingAdapter): SSLContext =
    try {
      def loadKeystore(filename: String, password: String): KeyStore = {
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
        val fin = Files.newInputStream(Paths.get(filename))
        try keyStore.load(fin, password.toCharArray) finally Try(fin.close())
        keyStore
      }

      val keyManagers = {
        val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        factory.init(loadKeystore(SSLKeyStore, SSLKeyStorePassword), SSLKeyPassword.toCharArray)
        factory.getKeyManagers
      }
      val trustManagers = {
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        trustManagerFactory.init(loadKeystore(SSLTrustStore, SSLTrustStorePassword))
        trustManagerFactory.getTrustManagers
      }

      val rng = createSecureRandom(log)

      val ctx = SSLContext.getInstance(SSLProtocol)
      ctx.init(keyManagers, trustManagers, rng)
      ctx
    } catch {
      case e: FileNotFoundException    ⇒ throw new RemoteTransportException("Server SSL connection could not be established because key store could not be loaded", e)
      case e: IOException              ⇒ throw new RemoteTransportException("Server SSL connection could not be established because: " + e.getMessage, e)
      case e: GeneralSecurityException ⇒ throw new RemoteTransportException("Server SSL connection could not be established because SSL context could not be constructed", e)
    }

  def createSecureRandom(log: MarkerLoggingAdapter): SecureRandom = {
    val rng = SSLRandomNumberGenerator match {
      case r @ ("AES128CounterSecureRNG" | "AES256CounterSecureRNG") ⇒
        log.debug("SSL random number generator set to: {}", r)
        SecureRandom.getInstance(r, AkkaProvider)
      case s @ ("SHA1PRNG" | "NativePRNG") ⇒
        log.debug("SSL random number generator set to: {}", s)
        // SHA1PRNG needs /dev/urandom to be the source on Linux to prevent problems with /dev/random blocking
        // However, this also makes the seed source insecure as the seed is reused to avoid blocking (not a problem on FreeBSD).
        SecureRandom.getInstance(s)

      case "" ⇒
        log.debug("SSLRandomNumberGenerator not specified, falling back to SecureRandom")
        new SecureRandom

      case unknown ⇒
        log.warning(LogMarker.Security, "Unknown SSLRandomNumberGenerator [{}] falling back to SecureRandom", unknown)
        new SecureRandom
    }
    rng.nextInt() // prevent stall on first access
    rng
  }
}

/**
 * INTERNAL API
 *
 * Used for adding SSL support to Netty pipeline
 */
private[akka] object NettySSLSupport {

  Security addProvider AkkaProvider

  def apply(settings: SSLSettings, log: MarkerLoggingAdapter, isClient: Boolean): SslHandler = apply(settings, log, isClient, None)
  /**
   * Construct a SSLHandler which can be inserted into a Netty server/client pipeline
   */
  def apply(settings: SSLSettings, log: MarkerLoggingAdapter, isClient: Boolean, remoteAddress: Option[Address]): SslHandler = {
    val context = settings.getOrCreateContext(log)
    val sslEngine = remoteAddress match {
      case Some(address) if address.hasGlobalScope && settings.SSLRequireHostnameValidation ⇒ context.createSSLEngine(address.host.get, address.port.get)
      case _ ⇒ context.createSSLEngine()
    }

    val sslParams = new SSLParameters()
    if (!isClient && settings.SSLRequireMutualAuthentication) sslParams.setNeedClientAuth(true)

    if (settings.SSLRequireHostnameValidation && (remoteAddress exists (_.hasGlobalScope)))
      // If we don't have remote host address, like when called for the server side, and
      // enable endpoint verification the handshake will fail with
      // "fatal error: 80: problem unwrapping net record"
      sslParams.setEndpointIdentificationAlgorithm("HTTPS")

    sslEngine.setSSLParameters(sslParams)
    sslEngine.setUseClientMode(isClient)
    sslEngine.setEnabledCipherSuites(settings.SSLEnabledAlgorithms.toArray)
    sslEngine.setEnabledProtocols(Array(settings.SSLProtocol))
    new SslHandler(sslEngine)
  }
}
