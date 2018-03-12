package com.orco.spark.deploy

/**
  * Used to send state on-the-wire about Executors from Worker to Master.
  * This state is sufficient for the Master to reconstruct its internal data structures during
  * failover.
  */
private[deploy] class ExecutorDescription(
                                           val appId: String,
                                           val execId: Int,
                                           val cores: Int,
                                           val state: ExecutorState.Value)
  extends Serializable {

  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(appId, execId, cores, state)
}

