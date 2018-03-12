package com.orco.spark

import java.util.concurrent.ConcurrentHashMap
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import com.orco.spark.internal.Logging
import com.orco.spark.util.Utils

class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import SparkConf._
  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()
  settings.put("spark.deploy.zookeeper.url","localhost:2181")
//  settings.put("spark.deploy.zookeeper.url",",node1:2181,node2:2181,node3:2181")
  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }
  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }
  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }
  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }
  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  /**
    * Looks for available deprecated keys for the given config option, and return the first
    * value available.
    */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }
  /**
    * Get a time parameter as milliseconds, falling back to a default if not set. If no
    * suffix is provided then milliseconds are assumed.
    */
  def getTimeAsMs(key: String, defaultValue: String): Long = {
    Utils.timeStringAsMs(get(key, defaultValue))
  }
  /**
    * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then seconds are assumed.
    * @throws java.util.NoSuchElementException If the time parameter is not set
    */
  def getTimeAsSeconds(key: String): Long = {
    Utils.timeStringAsSeconds(get(key))
  }
  /**
    * Get a time parameter as seconds, falling back to a default if not set. If no
    * suffix is provided then seconds are assumed.
    */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }
  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }
  /** Copy this object */
  override def clone: SparkConf = {
    val cloned = new SparkConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    set(key, value, false)
  }
}
private object SparkConf extends Logging {
  /**
    * Maps deprecated config keys to information about the deprecation.
    *
    * The extra information is logged as a warning when the config is present in the user's
    * configuration.
    */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.cache.class", "0.8",
        "The spark.cache.class property is no longer being used! Specify storage levels using " +
          "the RDD.persist() method instead."),
      DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
        "Please use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
        "Please use spark.kryoserializer.buffer instead. The default value for " +
          "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
          "are no longer accepted. To specify the equivalent now, one may use '64k'."),
      DeprecatedConfig("spark.rpc", "2.0", "Not used any more."),
      DeprecatedConfig("spark.scheduler.executorTaskBlacklistTime", "2.1.0",
        "Please use the new blacklisting options, spark.blacklist.*"),
      DeprecatedConfig("spark.yarn.am.port", "2.0.0", "Not used any more"),
      DeprecatedConfig("spark.executor.port", "2.0.0", "Not used any more"),
      DeprecatedConfig("spark.shuffle.service.index.cache.entries", "2.3.0",
        "Not used any more. Please use spark.shuffle.service.index.cache.size")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }
  /**
    * Maps a current config key to alternate keys that were used in previous version of Spark.
    *
    * The alternates are used in the order defined in this map. If deprecated configs are
    * present in the user's configuration, a warning is logged.
    *
    * TODO: consolidate it with `ConfigBuilder.withAlternative`.
    */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "spark.executor.userClassPathFirst"
      -> Seq(AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    "spark.history.fs.update.interval"
      -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
      AlternateConfig("spark.history.fs.updateInterval", "1.3"),
      AlternateConfig("spark.history.updateInterval", "1.3"))
  )

  /**
    * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
    * config keys.
    *
    * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
    */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
    * Logs a warning message if the given config key is deprecated.
    */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
    if (key.startsWith("spark.akka") || key.startsWith("spark.ssl.akka")) {
      logWarning(
        s"The configuration key $key is not supported any more " +
          s"because Spark doesn't use Akka since 2.0")
    }
  }
  /**
    * Holds information about keys that have been deprecated and do not have a replacement.
    *
    * @param key The deprecated key.
    * @param version Version of Spark where key was deprecated.
    * @param deprecationMessage Message to include in the deprecation warning.
    */
  private case class DeprecatedConfig(
                                       key: String,
                                       version: String,
                                       deprecationMessage: String)
  /**
    * Information about an alternate configuration key that has been deprecated.
    *
    * @param key The deprecated config key.
    * @param version The Spark version in which the key was deprecated.
    * @param translation A translation function for converting old config values into new ones.
    */
  private case class AlternateConfig(
                                      key: String,
                                      version: String,
                                      translation: String => String = null)
}