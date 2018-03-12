package com.orco.spark.rpc.netty


import com.orco.spark.internal.Logging
import com.orco.spark.network.client.RpcResponseCallback
import com.orco.spark.rpc.{RpcAddress, RpcCallContext}

import scala.concurrent.Promise

private[netty] abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext with Logging {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }

}

/**
  * If the sender and the receiver are in the same process, the reply can be sent back via `Promise`.
  */
private[netty] class LocalNettyRpcCallContext(
                                               senderAddress: RpcAddress,
                                               p: Promise[Any])
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

/**
  * A [[com.orco.spark.rpc.RpcCallContext]] that will call [[com.orco.spark.network.client.RpcResponseCallback]] to send the reply back.
  */
private[netty] class RemoteNettyRpcCallContext(
                                                nettyEnv: NettyRpcEnv,
                                                callback: RpcResponseCallback,
                                                senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}
