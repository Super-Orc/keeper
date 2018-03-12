package com.orco.spark.util

import com.orco.spark.internal.Logging

/**
  * The default uncaught exception handler for Spark daemons. It terminates the whole process for
  * any Errors, and also terminates the process for Exceptions when the exitOnException flag is true.
  *
  * @param exitOnUncaughtException Whether to exit the process on UncaughtException.
  */
private[spark] class SparkUncaughtExceptionHandler(val exitOnUncaughtException: Boolean = true)
  extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      // Make it explicit that uncaught exceptions are thrown when container is shutting down.
      // It will help users when they analyze the executor logs
//      val inShutdownMsg = if (ShutdownHookManager.inShutdown()) "[Container in shutdown] " else ""
      val errMsg = "Uncaught exception in thread "
//      logError(inShutdownMsg + errMsg + thread, exception)
      logError( errMsg + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
//      if (!ShutdownHookManager.inShutdown()) {
//        if (exception.isInstanceOf[OutOfMemoryError]) {
//          System.exit(SparkExitCode.OOM)
//        } else if (exitOnUncaughtException) {
//          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
//        }
//      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}

