package com.orco.test

class TestClassObj {

  /**
    * 这也行1
    */
  start()

  def start(): Unit ={
    println(11)
  }
  def bas(): Unit ={
    println(22)
  }
}
object Asa{
  def main(args: Array[String]): Unit = {

  new TestClassObj
  }
}