package com.ahuoo.nextetl.crawl

import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ListBuffer

object CrawlerTest {
  def main(sysArgs: Array[String]): Unit = {

    val code = "163407"
    val url = s"http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=$code&per=1000"
    val response: HttpResponse[String] = Http(url).param("page","1").asString
    //println(response.body)


    val pagePattern = "pages:(\\d+)".r
    val matches = pagePattern.findAllIn(response.body).matchData.toList
    val totalPage = matches(0).group(1).toInt

    var urlList = new ListBuffer[String]()
    for(page <- 1 to 2){
      val url = s"http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&per=1000&code=$code&page=$page"
      urlList += url
    }

    var valueList = new ListBuffer[(String, Double)]()
    val resultMap = urlList.map(url => {
      println(url)
      val content = Http(url).asString.body
      val dataPattern = "<td>([\\d|-]+)</td><td class='tor bold'>(.+?)</td>".r
      val matchList = dataPattern.findAllIn(response.body).matchData.toList
      for (elem <- matchList) {
        val (date, value) = (elem.group(1).toString, elem.group(2).toDouble)
        println(date)
        valueList += ((date, value))
      }
    })

    println(valueList.map(t => t._2))

  }
}
