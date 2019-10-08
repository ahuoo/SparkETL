package com.ahuoo.nextetl.learn.scala

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

/**
  *https://pedrorijo.com/blog/scala-exceptions/
  */
object ExceptionTest {
  def main(args: Array[String]) {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException =>{
        println("Missing file exception")
      }

      case ex: IOException => {
        println("IO Exception")
      }
      case _: Throwable => // do stuff
    }
  }
}