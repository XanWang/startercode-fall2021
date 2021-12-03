import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Test1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("test")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().appName(s"test").getOrCreate()

        val df = spark
                .read
                .option("sep", "\t")
                .csv("/Users/admin/Documents/retailclean")
        val source = df.rdd.filter(x => !x.getString(6).equals("CustomerID"))

        source.map(x => {
            val customId = x.getString(6)
            val price = x.getString(5)
            (customId.toInt, price.toFloat)
        }).groupByKey().map(x => {
            val l = x._2.toList
            val v = l.sum * 1.0 / l.size
            (x._1, v)
        }).saveAsTextFile("/Users/admin/Documents/q1")


        source.map(x => {
            val customId = x.getString(6)
            val country = x.getString(7)
            (customId, country)
        }).groupByKey().map(x => {
            (x._1, x._2.toList.distinct.size)
        }).saveAsTextFile("/Users/admin/Documents/q2")

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
