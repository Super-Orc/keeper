package com.orco.spark.deploy

private[deploy] case class DriverDescription(
                                              jarUrl: String,
                                              mem: Int,
                                              cores: Int,
                                              supervise: Boolean,
                                              command: Command) {

  override def toString: String = s"DriverDescription (${command.mainClass})"
}
