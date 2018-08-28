package sparksql

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.Row



object sparkjoins extends {
  case class empl(emp_id: Int, name: String, salary: Float)
case class profl(emp_id: Int, dept_id: Int, profession: String)
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
    //  implicits are important to work on Spark SQL
       import spark.implicits._

    // If you read in below manner, we'll get RDDs
    val emp = sc.textFile("src/test/resources/datasets/employee.txt", 2).map(x => x.split(","))
    val prof = sc.textFile("src/test/resources/datasets/employee_professions.txt", 2).map(y => y.split(","))

    val empdf = emp.map(x => empl(x(0).toInt, x(1), x(2).toFloat)).toDF()
    val profdf = prof.map(y => profl(y(0).toInt, y(1).toInt, y(2))).toDF()

    println("DataFrame Inner Join")
    val innerdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "inner")
    innerdf.collect().foreach(println)
    println("DataFrame Left Outer Join")
    val leftdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "left_outer")
    leftdf.collect().foreach(println)
    println("DataFrame Right Outer Join")
    val rightdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "right_outer")
    rightdf.collect.foreach(println)

    //If you read in below manner, we'll get DataFrames
    val empfile = spark.read.text("src/test/resources/datasets/employee.txt")
    val proffile = spark.read.text("src/test/resources/datasets/employee_professions.txt")

    empfile.printSchema()
    proffile.printSchema()

  }
}