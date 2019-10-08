package com.ahuoo.nextetl.learn.scala

object ListTest {
  def main(args: Array[String]): Unit ={
    //create list
    val listLispStyle = 1 :: 2 :: 3 :: Nil
    val listJavaStyle = List(1,2,3)
    val listRange = List.range(1,10)
    val listRangeStep = List.range(0,10,2)
    val listFill = List.fill(3)("foo")

    //create new list adding new element
    val newList = 4 :: listLispStyle
    println(newList)

    //merge two lists
    val a = List(1,5,3)
    val b = List(4,2,6)
    val mergeList = a ::: b
    println(mergeList)

    //iterator a list,  foreach and for
    mergeList.foreach{
      println(_)
    }
    for(value <- mergeList if value>4){
      println(value)
    }

    //filter
    println(mergeList.filter(x => x>2))

    //map ,transformation
    println(mergeList.map(x => x*2))

    //sort list
    println(mergeList.sortWith(_<_))





  }
}
