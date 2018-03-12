package com.orco.spark.util

import com.orco.spark.SparkConf
import com.orco.spark.rpc.RpcTimeout


private[spark] object RpcUtils {


  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: SparkConf): Int = {
    conf.getInt("spark.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: SparkConf): Long = {
    conf.getTimeAsMs("spark.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.askTimeout", "spark.network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq("spark.rpc.lookupTimeout", "spark.network.timeout"), "120s")
  }
}