package com.orco.spark.deploy.master


private[deploy] class MasterInfo(val id: String,
                                 val host: String,
                                 val port: Int)
  extends Serializable {

}
