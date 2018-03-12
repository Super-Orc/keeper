package com.orco.spark.deploy.master


import java.nio.ByteBuffer
import java.util

import com.orco.spark.SparkConf
import com.orco.spark.deploy.SparkCuratorUtil
import com.orco.spark.internal.Logging
import com.orco.spark.serializer.Serializer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode


private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
    with Logging {

  private val ZK_DIR = conf.get("spark.deploy.zookeeper.dir", "/test")
  private val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/test") + "/master_status"
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)


  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  /**
    * master 节点是否存在
    * @param path
    * @return true : master 存在
    */
  override def checkExists(path: String): Boolean = {
    zk.getChildren.forPath(WORKING_DIR).asScala.exists(_.startsWith("master-"))
  }

  override def getChildPathName(path: String): Int = {
    val list: util.List[String] = zk.getChildren.forPath(WORKING_DIR)
    list.asScala.filterNot(_.startsWith(path)).size
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(WORKING_DIR).asScala
      .filter(_.startsWith(prefix)).flatMap(deserializeFromFile[T])
  }

  override def close() {
    zk.close()
  }

  override def cleanZK(): Unit ={
    SparkCuratorUtil.deleteRecursive(zk, ZK_DIR + "/spark_leader")
    SparkCuratorUtil.deleteRecursive(zk, ZK_DIR + "/master_status")
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
    }
  }
}
