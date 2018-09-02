package scalaclasses.sessions
import org.apache.spark.sql.SparkSession
object firstscalaclass {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName("mapExample")
      .master("local")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6))

    val broadcast = spark.sparkContext.broadcast(data.collect())
    val x = broadcast.value
    println(x.mkString("|"))

  }
}