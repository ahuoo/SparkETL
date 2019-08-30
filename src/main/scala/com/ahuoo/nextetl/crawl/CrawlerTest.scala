package com.ahuoo.nextetl.crawl

import org.apache.log4j.Logger
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ListBuffer

object CrawlerTest {
  @transient lazy val log = Logger.getLogger(this.getClass)

  def main(sysArgs: Array[String]): Unit = {
    val code = "163407"
    val startDate = "2019-05-24"
    val valueList = getNetValue(code, startDate)

  }

  private def getNetValue(code: String, startDate: String) = {
    val url = s"http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=$code&per=1000"
    val response: HttpResponse[String] = Http(url).param("page", "1").asString
    //println(response.body)
    val pagePattern = "pages:(\\d+)".r
    val matches = pagePattern.findAllIn(response.body).matchData.toList
    val totalPage = matches(0).group(1).toInt
    var urlList = new ListBuffer[String]()
    var valueList = new ListBuffer[(String, Double)]()
    var currentDate = "2088-01-01"
    var page = 1
    while (page <= totalPage && currentDate >= startDate) {
      val url = s"http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&per=1000&code=$code&page=$page&sdate=$startDate"
      urlList += url
      val content = Http(url).asString.body
      val dataPattern = "<td>([\\d|-]+)</td><td class='tor bold'>(.+?)</td>".r
      val matchList = dataPattern.findAllIn(content).matchData.toList
      for (elem <- matchList) {
        val (date, value) = (elem.group(1).toString, elem.group(2).toDouble)
        //println(date,value)
        valueList += ((date, value))
        currentDate = date
      }
      page = page + 1
    }
    valueList.map(t => log.info(t._1+", "+ t._2))
    valueList
  }
}
