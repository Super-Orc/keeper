package com.orco.spark.rpc

private[rpc] class RpcEnvStoppedException()
  extends IllegalStateException("RpcEnv already stopped.")
