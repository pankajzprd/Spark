package scala.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager, ResultSet }
import org.apache.spark.storage.StorageLevel._

object sparkHive {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
     // .config("hive.metastore.uris", "thrift://nn01.itversity.com:9083")
      .appName("Spark Hive Integration")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    val sc = spark.sparkContext

    val hv = new org.apache.spark.sql.hive.HiveContext(sc)
    println("Printing")
    hv.read.table("default.yelp_user").show()
  //  spark.sql("Select * from default.yelp_user limit 10").collect.foreach(println)
    //    val NewDF = hiveContext.load(
    //      "jdbc",
    //      Map(
    //        "driver" -> "org.apache.hive.jdbc.HiveDriver",
    //        "url" -> "jdbc:hive2://nn01.itversity.com:10000/trainingv",
    //        "user" -> "trainingv",
    //        "password" -> "feetie7ji0a2phecha0kuiB9Ieke7Nei"))
    //
    //    NewDF.show()
  }
}