package com.orco.spark.deploy


import com.orco.spark.SparkConf
import com.orco.spark.internal.Logging

import scala.collection.JavaConverters._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException

private[spark] object SparkCuratorUtil extends Logging {

  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  private val RETRY_WAIT_MILLIS = 5000
  private val MAX_RECONNECT_ATTEMPTS = 3

  var zk: CuratorFramework = _
  private var WORKING_DIR :String= _

  def newClient(
                 conf: SparkConf,
                 zkUrlConf: String = "spark.deploy.zookeeper.url"): CuratorFramework = {
    WORKING_DIR= conf.get("spark.deploy.zookeeper.dir", "/test") + "/master_status"
    val ZK_URL = conf.get(zkUrlConf)
    zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }

  def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
        // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  def deleteRecursive(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path).asScala) {
        zk.delete().forPath(path + "/" + child)
      }
      //      zk.delete().forPath(path)
    }
  }

  /**
    * master 节点是否存在
    *
    * @param path
    * @return true : master 存在
    */
  def checkExists(path: String): Boolean = {
    zk.getChildren.forPath(WORKING_DIR).asScala.exists(_.startsWith("master-"))
  }
}

