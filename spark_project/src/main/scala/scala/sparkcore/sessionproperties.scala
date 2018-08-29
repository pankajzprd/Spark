package sparkcore
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

trait sessionproperties {
  /*
   * To work on Spark on Windows OS, we need to specify below property.
   * This property points to hadoop home
   */
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  val spark = SparkSession
    .builder()
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .appName("Spark examples")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))
}