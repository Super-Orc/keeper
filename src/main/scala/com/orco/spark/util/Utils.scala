package com.orco.spark.util

import java.io.{File, IOException}
import java.lang.management.ManagementFactory
import java.math.{MathContext, RoundingMode}
import java.net.{BindException, URI}
import java.nio.file.{Files, Paths}
import java.util.Locale

import com.orco.spark.{SparkConf, SparkException}
import com.orco.spark.internal.Logging
import com.orco.spark.network.util.JavaUtils
import io.netty.channel.unix.Errors.NativeIoException
import org.slf4j.Logger

import scala.util.control.{ControlThrowable, NonFatal}


/**
  * Various utility methods used by Spark.
  */
private[spark] object Utils extends Logging {

  private val sparkUncaughtExceptionHandler = new SparkUncaughtExceptionHandler

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }
  /**
    * Convert a quantity in megabytes to a human-readable string such as "4.0 MB".
    */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }
  /**
    * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
    * default UncaughtExceptionHandler
    *
    * NOTE: This method is to be called by the spark-started JVM process.
    */
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => sparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }
  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /**
    * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
    */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    if (size >= BigInt(1L << 11) * EB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EB) {
          (BigDecimal(size) / EB, "EB")
        } else if (size >= 2 * PB) {
          (BigDecimal(size) / PB, "PB")
        } else if (size >= 2 * TB) {
          (BigDecimal(size) / TB, "TB")
        } else if (size >= 2 * GB) {
          (BigDecimal(size) / GB, "GB")
        } else if (size >= 2 * MB) {
          (BigDecimal(size) / MB, "MB")
        } else if (size >= 2 * KB) {
          (BigDecimal(size) / KB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }
  /**
    * Check to see if file is a symbolic link.
    */
  def isSymlink(file: File): Boolean = {
    return Files.isSymbolicLink(Paths.get(file.toURI))
  }
  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }
  /**
    * Execute the given block, logging and re-throwing any uncaught exception.
    * This is particularly useful for wrapping code that runs in a thread, to ensure
    * that exceptions are printed, and to avoid having to catch Throwable.
    */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  def checkHost(host: String) {
    assert(host != null && host.indexOf(':') == -1, s"Expected hostname (not IP) but got $host")
  }
  def checkHostPort(hostPort: String) {
    assert(hostPort != null && hostPort.indexOf(':') != -1,
      s"Expected host and port but got $hostPort")
  }

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Utility function that should be called early in `main()` for daemons to set up some common
    * diagnostic state.
    */
  def initDaemon(log: Logger): Unit = {
    log.info(s"Started daemon with process name: ${Utils.getProcessName()}")
    SignalUtils.registerLogger(log)
  }

  /**
    * Returns the name of this JVM process. This is OS dependent but typically (OSX, Linux, Windows),
    * this is formatted as PID@hostname.
    */
  def getProcessName(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }
  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in ms.
    */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }
  /**
    * Attempt to start a service on the given port, or fail after a number of attempts.
    * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
    *
    * @param startPort The initial port to start the service on.
    * @param startService Function to start service on a given port.
    *                     This is expected to throw java.net.BindException on port collision.
    * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
    * @param serviceName Name of the service.
    * @return (service: T, port: Int)
    */
  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             conf: SparkConf,
                             serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        userPort(startPort, offset)
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = if (startPort == 0) {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (on a random free port)! " +
                s"Consider explicitly setting the appropriate binding address for " +
                s"the service$serviceString (for example spark.driver.bindAddress " +
                s"for SparkDriver) to the correct binding address."
            } else {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service$serviceString (for example spark.ui.port " +
                s"for SparkUI) to an available port or increasing spark.port.maxRetries."
            }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          if (startPort == 0) {
            // As startPort 0 is for a random free port, it is most possibly binding address is
            // not correct.
            logWarning(s"Service$serviceString could not bind on a random free port. " +
              "You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
          }
      }
    }
    // Should never happen
    throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
    * Return whether the exception is caused by an address-port collision when binding.
    */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
//      case e: MultiException =>
//        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
    * Returns the user port to try when trying to bind a service. Handles wrapping and skipping
    * privileged ports.
    */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }


  /**
    * Maximum number of retries when binding to a port before giving up.
    */
  def portMaxRetries(conf: SparkConf): Int = {
    val maxRetries = conf.getOption("spark.port.maxRetries").map(_.toInt)
    if (conf.contains("spark.testing")) {
      // Set a higher number of retries for tests...
      maxRetries.getOrElse(100)
    } else {
      maxRetries.getOrElse(16)
    }
  }
  /**
    * A file name may contain some invalid URI characters, such as " ". This method will convert the
    * file name to a raw path accepted by `java.net.URI(String)`.
    *
    * Note: the file name must not contain "/" or "\"
    */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
  }
  /**
    * Return a pair of host and port extracted from the `sparkUrl`.
    *
    * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
    * host and port.
    *
    *  org.apache.spark.SparkException if sparkUrl is invalid.
    */
  @throws(classOf[SparkException])
  def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new SparkException("Invalid master URL: " + sparkUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid master URL: " + sparkUrl, e)
    }
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in seconds.
    */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }
}

