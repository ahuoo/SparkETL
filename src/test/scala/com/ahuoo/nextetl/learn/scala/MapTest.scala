package com.ahuoo.nextetl.learn.scala

object MapTest {
    def main(args: Array[String]): Unit ={
      // create a map with initial elements
      var states = scala.collection.mutable.Map("AL" -> "Alabama", "AK" -> "Alaska")

      // add elements with +=
      states += ("AZ" -> "Arizona")
      states += ("CO" -> "Colorado", "KY" -> "Kentucky")

      // remove elements with -=
      states -= "KY"
      states -= ("AZ", "CO")

      // update elements by reassigning them
      states("AK") = "Alaska, The Big State"

      //iterator map
      for ((k,v) <- states){
        printf(" %s = %s\n", k, v)
      }

      // version 1 (tuples)
      //states foreach (x => println (x._1 + "-->" + x._2))

      // version 2 (foreach and case)
      //states foreach {case (key, value) => println (key + "-->" + value)}

      //only show keys or values
      states.keys.foreach(k => println(k))
      states.values.foreach(v => println(v))

      //keys to list
      val list = states.keys.toList
      println(list)
    }
}
