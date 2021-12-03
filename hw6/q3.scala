import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object q3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(s"test").getOrCreate()

    val df1 = spark
      .read
      .option("sep", "\t")
      .csv("/Users/admin/Documents/facebook")
    val rdd1 = df1.rdd.map(x => {
      (x.getString(1), x.getString(0))
    })

    rdd1.join(rdd1).filter(x => {
      x._2._1 < x._2._2
    }).map(x => (x._2, x._1))
      .groupByKey()
      .map(x => {
        (x._1._1, x._2.toList)
      }).groupByKey().map(x => {
      val l = x._2.toList
      val arr = new ArrayBuffer[Int]()
      for (i <- l) {
        for (j <- i) {
          arr.append(j.toInt)
        }
      }
      (x._1, arr.distinct.sum)
    }).saveAsTextFile("/Users/admin/Documents/q3")
  }
}
