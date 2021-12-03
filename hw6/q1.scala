import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object q1 {
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
  }
}
