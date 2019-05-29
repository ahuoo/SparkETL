package com.moodys.nextgen

/**
  * https://alvinalexander.com/scala/best-practice-option-some-none-pattern-scala-idioms
  */
object ScalaTest {
  def main(sysArgs: Array[String]): Unit = {
    val x = toInt("1")
    val z = toInt("hello")
    println(x)
    println(z)
    //1
    val x2 = toInt("1").getOrElse(0)
    println(x2)
    //2
    toInt("1").foreach{ i =>
      println(s"$i")
    }
    //3
    toInt("1") match {
      case Some(i) => println(i)
      case None => println("That didn't work.")
    }

    //test Option
    val bag = List("1", "2", "foo", "3", "bar")
    bag.map(toInt)
    bag.flatMap(toInt)
    bag.map(toInt).collect{case Some(i) => i}


  }
  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      case e: Exception => None
    }
  }

}
