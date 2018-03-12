package com.orco.spark.deploy

sealed trait IMessage extends Serializable

private[deploy] object IMessage {

  case object MyRequest

  case class MyResponse(name: String, age: Int)

}
