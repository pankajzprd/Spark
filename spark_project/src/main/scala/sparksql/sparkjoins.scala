package sparksql

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.Row

object sparkjoins extends {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      //  below configuration is set due to "Relative path in absolute URI error"
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .appName("Spark examples")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val schema = StructType(
      StructField("emp_id", IntegerType, false) ::
        StructField("name", StringType, true) ::
        StructField("salary", FloatType, true) :: Nil)
    // If you read in below manner, we'll get RDDs
    val emp = sc.textFile("src/test/resources/datasets/employee.txt", 2).map(x => x.split(","))
    val prof = sc.textFile("src/test/resources/datasets/employee_professions.txt", 2).map(y => y.split(","))
    val emprow = emp.map(element => Row(element))
    val df = spark.sqlContext.createDataFrame(emprow, schema)
    //If you read in below manner, we'll get DataFrames
    val empfile = spark.read.text("src/test/resources/datasets/employee.txt")
    val proffile = spark.read.text("src/test/resources/datasets/employee_professions.txt")
    df.printSchema()
    empfile.printSchema()
    proffile.printSchema()

  }
}