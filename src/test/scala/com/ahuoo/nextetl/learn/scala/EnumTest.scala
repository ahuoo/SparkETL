package com.ahuoo.nextetl.learn.scala

/**
  * http://allaboutscala.com/tutorials/chapter-2-learning-basics-scala-programming/learn-to-create-use-enumerations/
  */
object EnumTest extends Enumeration {
  type EnumTest = Value

  val Glazed      = Value("Glazed")
  val Strawberry  = Value("Strawberry")
  val Plain       = Value("Plain")
  val Vanilla     = Value("Vanilla")

  val Ok          = Value(-1, "Ok")

  def main(args: Array[String]): Unit ={
    EnumTest.values.foreach(println)

    println("\nStep 2: How to print the String value of the enumeration")
    println(s"Vanilla Donut string value = ${EnumTest.Vanilla}")

    println("\nStep 3: How to print the id of the enumeration")
    println(s"Vanilla Donut's id = ${EnumTest.Vanilla.id}")

    println("\nStep 4: How to print all the values listed in Enumeration")
    println(s"Donut types = ${EnumTest.values}")

    println("\nStep 5: How to pattern match on enumeration values")
    EnumTest.values.foreach {
      case d if (d == EnumTest.Strawberry || d == EnumTest.Glazed) => println(s"Found favourite donut = $d")
      case _ => None
    }
  }
}