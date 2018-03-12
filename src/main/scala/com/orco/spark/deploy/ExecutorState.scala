package com.orco.spark.deploy

private[deploy] object ExecutorState extends Enumeration {

  val LAUNCHING, RUNNING, KILLED, FAILED, LOST, EXITED = Value

  type ExecutorState = Value

  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST, EXITED).contains(state)
}
