package com.orco.spark.deploy.master

private[master] object WorkerState extends Enumeration {
  type WorkerState = Value

  val ALIVE, DEAD, DECOMMISSIONED, UNKNOWN = Value
}
