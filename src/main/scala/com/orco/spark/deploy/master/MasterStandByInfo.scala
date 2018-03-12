package com.orco.spark.deploy.master


private[deploy] class MasterStandByInfo(val id: String,
                                 val host: String,
                                 val port: Int)
  extends Serializable {

}

