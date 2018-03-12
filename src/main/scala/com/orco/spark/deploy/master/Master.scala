package com.orco.spark.deploy.master

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import com.orco.spark.SparkConf
import com.orco.spark.deploy.DeployMessages._
import com.orco.spark.deploy.master.MasterMessages._
import com.orco.spark.internal.Logging
import com.orco.spark.rpc._
import com.orco.spark.serializer.JavaSerializer
import com.orco.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}

import com.orco.spark.deploy.{SparkCuratorUtil, WatchLevel}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

private[deploy] class Master(
                              val rpcEnv: RpcEnv,
                              address: RpcAddress,
                              host: String,
                              port: Int,
                              val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  /**
    * some param must be done when master changed
    */
  private var masterId: String = _
  private var masterHost :String= _
  private var masterPort :Int= _


  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4
  val masterSbSet = new mutable.HashSet[MasterStandByInfo]
  private val idToMasterSb = new mutable.HashMap[String, MasterStandByInfo]
  private val masterUrl = address.toSparkURL
  private val forwordMessageScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-masterStandBy-heartBeat")
  private var persistenceEngine: PersistenceEngine = _
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    1 // Make sure we can register with all masters at the same time
  )
  private var leaderElectionAgent: LeaderElectionAgent = _
  private var master: Option[RpcEndpointRef] = None //use by masterSb
  private var isMaster: Boolean = false //use by master

  private var registerMasterFutures: JFuture[_] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = 3L
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = math.round(60 * REGISTRATION_RETRY_FUZZ_MULTIPLIER)
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private var connectionAttemptCount = 0
  private var registeredToMaster = false
  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10


  //todo 待解决：如果终止程序，zk 路径下节点没有删除怎么办？是否涉及成每次启动，如果是 master，则递归删除目录下节点？persistenceEngine.cleanZK()
  override def onStart(): Unit = {

    val serializer: JavaSerializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = {
      val zkFactory =
        new ZooKeeperRecoveryModeFactory(conf, serializer)
      (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

    /**
      * 如果已经有了 master，则创建 standby,并与 master 心跳
      * 如何心跳？
      * 否则，创建 master
      */
    if (persistenceEngine.checkExists("master")) {
      masterId = generateMasterId("masterSb", address.host, address.port)
      masterHost = host
      masterPort = port
      // masterSb
      registerToMaster()
    } else {
      // master
      persistenceEngine.cleanZK()
      masterId = generateMasterId("master", address.host, address.port)
      persistenceEngine.addMaster(new MasterInfo(masterId, address.host, address.port))
      isMaster = true
      masterHost = address.host
      masterPort = address.port
    }

    //todo 其实这里应该发送一个 onStop 事件
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        if (isMaster) {
          persistenceEngine.removeMaster(masterId)
          logInfo("master killing... zookeeper persistence clean soon...")
        }
        else {
          persistenceEngine.removeMaster(masterId)
          logInfo("masterSb killing... zookeeper persistence clean soon...")
        }
      }
    })
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    //masterSb
    case msg: RegisterMasterSbResponse =>
      handleRegisterResponse(msg)

    case ElectedLeader =>
      logInfo("master.receive.ElectedLeader")

    //master
    case Heartbeat(masterSbId, masterSb) =>
      idToMasterSb.get(masterSbId) match {
        case Some(masterStandByInfo) =>
          println("M-Heartbeat-some")
          logInfo(s"master get a heartbeat signal $masterStandByInfo")
        case None =>
          println("M-Heartbeat-None")
          logError(s"Got heartbeat from unregistered masterSb $masterSb." +
            s" This masterSb was never registered, so i don't know how to handle this heartbeat. -->${WatchLevel.LEVEL_HIGH}<--")
      }
    //masterSb
    case SendHeartbeat => {
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            println("S-SendHeartbeat-registerMasterThreadPool")
            logInfo("SendHeartbeat: Connecting to master " + masterHost + ":" + masterPort + "...")
            //            val masterEndpoint = rpcEnv.setupEndpointRef(address, Master.ENDPOINT_NAME)
            //            master = Some(masterEndpoint)
            sendToMaster(Heartbeat(masterId, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterHost:$masterPort", e)
          }
        }
      })
    }

    //master
    case RegisterMasterSb(id, masterSbHost, masterSbPort, masterSbRef, masterAddress) =>
      logInfo("Registering by MasterSb %s:%d ".format(masterSbHost, masterSbPort))
      if (idToMasterSb.contains(id)) {
        println("M-RegisterMasterSb-if")
        masterSbRef.send(RegisterMasterSbFailed(s"Duplicate masterSb ID $id"))
      } else {
        println("M-RegisterMasterSb-else")
        val masterSb = new MasterStandByInfo(id, masterSbHost, masterSbPort)
        persistenceEngine.addMasterStandBy(masterSb)
        masterSbRef.send(RegisteredMasterSb(self, masterAddress))
        idToMasterSb(id) = masterSb
        masterSbSet += masterSb
      }

    //masterSb
    case ReregisterToMaster =>
      reRegisterToMaster()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case BoundPortsRequest => {
      logInfo("BoundPortsRequest->BoundPortsResponse")
      context.reply(BoundPortsResponse(address.port, 8080, null))
    }
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

  private def generateMasterId(name: String,
                               host: String,
                               port: Int): String = {
    "%s-%s-%s-%d".format(name, createDateFormat.format(new Date), host, port)
  }

  def cancelLastRegistrationRetry(): Unit = {
    println("S-cancelLastRegistrationRetry")
    if (registerMasterFutures != null) {
      registerMasterFutures.cancel(true)
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  //masterSb
  private def handleRegisterResponse(msg: RegisterMasterSbResponse): Unit = synchronized {
    msg match {
      case RegisteredMasterSb(masterRef, masterAddress) => {
        logInfo("Successfully registered to master " + masterAddress.toSparkURL)
        println("S-handleRegisterResponse-RegisteredMasterSb")
        registeredToMaster = true
        cancelLastRegistrationRetry()

        println("S-SendHeartbeat-forwordMessageScheduler")
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
      }
      case RegisterMasterSbFailed(message) =>
        logError("Worker registration failed: " + message)
        System.exit(1)
    }
  }


  //masterSb
  private def registerToMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        println("S-registerToMaster")
        registeredToMaster = false
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterToMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS + 3,
          TimeUnit.SECONDS))
      case Some(_) =>
        println("S-registerToMaster-some")
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  // masterSb
  private def tryRegisterAllMasters(): JFuture[_] = {
    registerMasterThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          println("S-tryRegisterAllMasters")
          logInfo(s"Connecting to master $masterHost:$masterPort...")
          val masterEndpoint = rpcEnv.setupEndpointRef(RpcAddress(masterHost,masterPort), Master.ENDPOINT_NAME)
          master = Some(masterEndpoint)
          sendRegisterMessageToMaster(masterEndpoint)
        } catch {
          case ie: InterruptedException => // Cancelled
          case NonFatal(e) => logWarning(s"Failed to connect to master $masterHost:$masterPort", e)
        }
      }
    })
  }

  private def reRegisterToMaster(): Unit = {
    Utils.tryOrExit {
      println("S-reRegisterToMaster")
      connectionAttemptCount += 1
      if (registeredToMaster) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.cancel(true)
            }
            val masterAddress = masterRef.address
            registerMasterFutures = registerMasterThreadPool.submit(new Runnable {
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
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.cancel(true)
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
                self.send(ReregisterToMaster)
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


  // masterSb
  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    println(address+":"+masterEndpoint.address)
    masterEndpoint.send(RegisterMasterSb(
      masterId,
      address.host,
      address.port,
      self,
      masterEndpoint.address))
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val host = InetAddress.getLocalHost.getHostAddress
    val port = 7077
    val standbyhost = InetAddress.getLocalHost.getHostAddress
    val standbyport = 7078
    SparkCuratorUtil.newClient(conf)
    val (rpcEnv, _, _) =
      if (SparkCuratorUtil.checkExists("master")) {
        //masterSb
        startRpcEnvAndEndpoint(standbyhost, standbyport, host, port, conf)
      } else {
        //master
        startRpcEnvAndEndpoint(host, port, null, 0, conf)
      }
    rpcEnv.awaitTermination()
  }

  /**
    * Start the Master and return a three tuple of:
    * (1) The Master RpcEnv
    * (2) The web UI bound port
    * (3) The REST server bound port, if any
    */
  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              hostSb: String,
                              portSb: Int,
                              conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, hostSb, portSb, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}