package com.orco.spark.deploy.master

import java.util.UUID

import com.orco.spark.SparkConf
import com.orco.spark.deploy.WatchLevel
import com.orco.spark.serializer.JavaSerializer
import com.orco.spark.util.Utils

object TestZk {
  var persistenceEngine: PersistenceEngine = _

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    val serializer: JavaSerializer = new JavaSerializer(conf)
//    persistenceEngine= new ZooKeeperPersistenceEngine(conf, serializer)
//
////    val workerInfo = new WorkerInfo("112","22",33333,2,1,null,"55")
////    persistenceEngine.addWorker(workerInfo)
////    val masterInfo = new MasterInfo("1112","22",33333,null)
////    persistenceEngine.checkExists("111")
////    persistenceEngine.addMaster(masterInfo)
//    println(persistenceEngine.getChildPathName("master"))
//
//
//    println(persistenceEngine.read("master_").head.asInstanceOf[MasterInfo].id)

//    Thread.sleep(Long.MaxValue)






    //================
    println(UUID.randomUUID.getMostSignificantBits)
  }
  a

  def a (): Unit ={
    println(111)
  }
}
