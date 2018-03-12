package com.orco.spark.rpc

import com.orco.spark.SparkException

private[rpc] class RpcEndpointNotFoundException(uri: String)
  extends SparkException(s"Cannot find endpoint: $uri")
