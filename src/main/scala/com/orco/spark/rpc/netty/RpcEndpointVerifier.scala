package com.orco.spark.rpc.netty

import com.orco.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}


/**
  * An [[com.orco.spark.rpc.RpcEndpoint]] for remote [[com.orco.spark.rpc.RpcEnv]]s to query if an `RpcEndpoint` exists.
  *
  * This is used when setting up a remote endpoint reference.
  */
private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

private[netty] object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an `RpcEndpoint` exists. */
  case class CheckExistence(name: String)
}

