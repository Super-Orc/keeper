package com.orco.spark.deploy.worker

import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}

import com.orco.spark.SparkConf
import com.orco.spark.deploy.DeployMessages._
import com.orco.spark.deploy.IMessage.{MyRequest, MyResponse}
import com.orco.spark.deploy.master.Master
import com.orco.spark.internal.Logging
import com.orco.spark.rpc._
import com.orco.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}

import scala.util.Random
import scala.util.control.NonFatal

private[deploy] class Worker(
                              override val rpcEnv: RpcEnv,
                              webUiPort: Int,
                              cores: Int,
                              memory: Int,
                              masterRpcAddresses: Array[RpcAddress],
                              endpointName: String,
                              workDirPath: String = null,
                              val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10

  /**
    * Whether to use the master address in `masterRpcAddresses` if possible. If it's disabled, Worker
    * will just use the address received from Master.
    */
  private val preferConfiguredMasterAddress =
    conf.getBoolean("spark.worker.preferConfiguredMasterAddress", false)
  /**
    * The master address to connect in case of failure. When the connection is broken, worker will
    * use this address to connect. This is usually just one of `masterRpcAddresses`. However, when
    * a master is restarted or takes over leadership, it will be an address sent from master, which
    * may not be in `masterRpcAddresses`.
    */

  private var masterAddressToConnect: Option[RpcAddress] = None
  private var registered = false
  private var activeMasterUrl: String = ""
  private[worker] var activeMasterWebUiUrl: String = ""
  private var master: Option[RpcEndpointRef] = None
  private var connected = false
  // A scheduled executor used to send messages at the specified time.
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4
  private val workerId = generateWorkerId()

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None
  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private var connectionAttemptCount = 0
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER)
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER)


  override def onStart() {
    assert(!registered)
    logInfo("ssssssssssStarting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    registerWithMaster()
  }

  /**
    * Re-register with the master because a network failure or a master failure has occurred.
    * If the re-registration attempt threshold is exceeded, the worker exits with error.
    * Note that for thread-safety this should only be called from the rpcEndpoint.
    */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress =
              if (preferConfiguredMasterAddress) masterAddressToConnect.get else masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    case SendHeartbeat => {
      logDebug(s"SendHeartbeat=${connected}")
      if (connected) {
        sendToMaster(Heartbeat(workerId, self))
      }
    }

    case ReregisterWithMaster => reregisterWithMaster()
    case MyRequest => println("my response test int worker's receive")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    //    case RequestWorkerState =>
    //      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
    //        finishedExecutors.values.toList, drivers.values.toList,
    //        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
    //        coresUsed, memoryUsed, activeMasterWebUiUrl))
    case MyRequest => {
      context.reply(MyResponse("Tom", 27))
    }
  }


  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
      case RegisteredWorker(masterRef, masterWebUiUrl, masterAddress) =>
        //        if (preferConfiguredMasterAddress) {
        logInfo("Successfully registered with master " + masterAddress.toSparkURL)
        //        } else {
        logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
        //        }
        registered = true
        changeMaster(masterRef, masterWebUiUrl, masterAddress)
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)

    }
  }

  /**
    * Change to use the new master.
    *
    * @param masterRef     the new master ref
    * @param uiUrl         the new master Web UI address
    * @param masterAddress the new master address which the worker should use to connect in case of
    *                      failure
    */
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String, masterAddress: RpcAddress) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    masterAddressToConnect = Some(masterAddress)
    master = Some(masterRef)
    connected = true
    //    if (reverseProxy) {
    //      logInfo(s"WorkerWebUI is available at $activeMasterWebUiUrl/proxy/$workerId")
    //    }
    //    // Cancel any outstanding re-registration attempts because we found a new master
    cancelLastRegistrationRetry()
  }

  /**
    * Cancel last registeration retry, or do nothing if no retry
    */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  /**
    * Send a message to the current master. If we have not yet registered successfully with any
    * master, the message will be dropped.
    */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }

  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            sendRegisterMessageToMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      self,
      cores,
      memory,
      null,
      masterEndpoint.address))
  }


}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val host = "localhost"
    val port = 7077
    val webUiPort = 8080
    val rpcEnv = startRpcEnvAndEndpoint(host, port, webUiPort, 2,
      2, Array("spark://localhost:7077"), null, conf = conf)
    // With external shuffle service enabled, if we request to launch multiple workers on one host,
    // we can only successfully launch the first worker and the rest fails, because with the port
    // bound, we may launch no more than one external shuffle service on each host.
    // When this happens, we should give explicit reason of failure instead of fail silently. For
    // more detail see SPARK-20989.
    val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)
    val sparkWorkerInstances = scala.sys.env.getOrElse("SPARK_WORKER_INSTANCES", "1").toInt
    require(externalShuffleServiceEnabled == false || sparkWorkerInstances <= 1,
      "Starting multiple workers on one host is failed because we may launch no more than one " +
        "external shuffle service on each host, please set spark.shuffle.service.enabled to " +
        "false or set SPARK_WORKER_INSTANCES to 1 to resolve the conflict.")

    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              webUiPort: Int,
                              cores: Int,
                              memory: Int,
                              masterUrls: Array[String],
                              workDir: String,
                              workerNumber: Option[Int] = None,
                              conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val rpcEnv = RpcEnv.create(systemName, host, port, conf)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    val a: RpcEndpointRef = rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, ENDPOINT_NAME, workDir, conf))
    Option(a).foreach(_.askSync[MyResponse](MyRequest))
    rpcEnv
  }

}
