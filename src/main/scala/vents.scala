package org.bigdata.application

import org.apache.spark._
import org.apache.log4j._
import scala.math.max

object vents {

  def parseLine(line:String): (String, Float, Float) = {
    val fields = line.split(",")
    val formapago1 = fields(0)
    val undies1 = fields(1).toFloat
    val base = fields(2).toFloat
    (formapago1, undies1, base)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "vents")
    val lines= sc.textFile("data/sales.csv")
    val parsedLines = lines.map(parseLine)
    val formapago = parsedLines.filter(x => x._1 == "Stripe")
    val uni = formapago.map(x => (x._1, x._3.toFloat))
    val re = uni.reduceByKey((x,y) => max(x,y))
    val results = re.collect()

    for (result <- results.sorted) {
      val fp = result._1
      val undid_r = result._2
      println("result :" + fp)
      println(undid_r)
    }


  }

}
