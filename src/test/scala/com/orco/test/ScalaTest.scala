package com.orco.test

class ScalaTest {

  def test(msg: String)(body: => Unit): Unit = {
    println(s"test suite: $msg")
    body
  }

  test("applyOrElse") {

    val onePF: PartialFunction[Int, String] = {
      case 1 => "One"
    }
    println(onePF.applyOrElse(1,{num:Int=>"two"}))
    println(onePF.applyOrElse(2,{num:Int=>"two"}))
  }

}

object ScalaTest {

  def main(args: Array[String]) {
    new ScalaTest
  }
}
