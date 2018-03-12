package com.orco.spark.deploy

private[deploy] object WatchLevel extends Enumeration {
  type WatchLevel = Value

  val LEVEL_LOW, LEVEL_HIGH = Value
}
