package com.orco.spark.deploy.master

import com.orco.spark.deploy.worker.Worker

object Main {
  def main(args: Array[String]): Unit = {
    Master.main(null)
    Worker.main(null)
  }

}
