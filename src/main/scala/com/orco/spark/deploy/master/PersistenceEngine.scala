package com.orco.spark.deploy.master

import com.orco.spark.rpc.RpcEnv

import scala.reflect.ClassTag


/**
  * Allows Master to persist any state that is necessary in order to recover from a failure.
  * The following semantics are required:
  *   - addApplication and addWorker are called before completing registration of a new app/worker.
  *   - removeApplication and removeWorker are called at any time.
  * Given these two requirements, we will have all apps and workers persisted, but
  * we might not have yet deleted apps or workers that finished (so their liveness must be verified
  * during recovery).
  *
  * The implementation of this trait defines how name-object pairs are stored or retrieved.
  */
abstract class PersistenceEngine {

  /**
    * Defines how the object is serialized and persisted. Implementation will
    * depend on the store used.
    */
  def persist(name: String, obj: Object): Unit

  /**
    * Defines how the object referred by its name is removed from the store.
    */
  def unpersist(name: String): Unit

  def checkExists(path: String): Boolean

  def cleanZK()

  /**
    * find index from childPath
    *
    * @param path
    * @return filePath's index
    */
  def getChildPathName(path: String): Int

  /**
    * Gives all objects, matching a prefix. This defines how objects are
    * read/deserialized back.
    */
  def read[T: ClassTag](prefix: String): Seq[T]

  final def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  final def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  final def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  final def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  final def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  final def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  final def addMaster(master: MasterInfo): Unit = {
    persist(master.id, master)
  }
//  final def addMaster(master: MasterInfo): Unit = {
//    persist("master", master)
//  }

  final def removeMaster(masterId: String): Unit = {
    unpersist(masterId)
  }
//  final def removeMaster(master: MasterInfo): Unit = {
//    unpersist("master")
//  }

  final def addMasterStandBy(masterStandBy: MasterStandByInfo): Unit = {
    persist(masterStandBy.id, masterStandBy)
    //    persist("masterStandBy_" + masterStandBy.id, masterStandBy)
  }

  final def removeMasterStandBy(masterStandBy: MasterStandByInfo): Unit = {
    unpersist(masterStandBy.id)
    //    unpersist("masterStandBy_" + masterStandBy.id)
  }

  /**
    * Returns the persisted data sorted by their respective ids (which implies that they're
    * sorted by time of creation).
    */
  final def readPersistedData(
                               rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
    }
  }

  def close() {}
}