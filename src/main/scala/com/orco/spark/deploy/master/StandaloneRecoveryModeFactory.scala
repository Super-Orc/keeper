package com.orco.spark.deploy.master


import com.orco.spark.SparkConf
import com.orco.spark.serializer.Serializer

/**
  * ::DeveloperApi::
  *
  * Implementation of this class can be plugged in as recovery mode alternative for Spark's
  * Standalone mode.
  *
  */
abstract class StandaloneRecoveryModeFactory(conf: SparkConf, serializer: Serializer) {

  /**
    * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
    * is handled for recovery.
    *
    */
  def createPersistenceEngine(): PersistenceEngine

  /**
    * Create an instance of LeaderAgent that decides who gets elected as master.
    */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
}


private[master] class ZooKeeperRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  def createPersistenceEngine(): PersistenceEngine = {
    new ZooKeeperPersistenceEngine(conf, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new ZooKeeperLeaderElectionAgent(master, conf)
  }
}
