import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object q2 {
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
      val country = x.getString(7)
      (customId, country)
    }).groupByKey().map(x => {
      (x._1, x._2.toList.distinct.size)
    }).saveAsTextFile("/Users/admin/Documents/q2")
  }
}
