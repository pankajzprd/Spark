package scala.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.{SQLContext, SaveMode}


object sparksqloperations {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
      .appName("Spark Dataset Joins")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val log = sc.setLogLevel("WARN")
    import spark.implicits._

    val employees = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/datasets/employee.csv")

    val professions = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/datasets/employee_professions.csv")

    println("Maximum salary based on Id")
    employees.groupBy(employees("id")).max("salary").show()
    println("Sum of salary based on Id")
    employees.groupBy("id").sum("salary").show()
    println("filter salary based on value")
    employees.filter(employees("salary") === 20000).show
    println("grouping and aggregating ")
    employees.groupBy("id")
      .agg(
        min(employees("id")),
        max(employees("id")),
        sum(employees("salary")))
      .show()

    employees.select("id", "name").collect.foreach(println)
    employees.createOrReplaceTempView("emp")
    println("sample sql")
    spark.sql("Select name, sum(salary) from emp group by name").show()

    employees.intersect(professions).show()
    employees.union(professions).show()
    employees.unionAll(professions).show()
    employees.except(professions).show()
    employees.distinct().show()

    println("JDBC Connectivity")
    val jdbcDF = spark.sqlContext.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://sample:1433;databasename=PROD")
      .option("dbtable", "clients")
      .option("user", "Readjust")
      .option("password", "None")
      .load()

    jdbcDF.printSchema()
    jdbcDF.select("ClientID", "CountryID", "ClientName", "ActiveFlag").show()
    jdbcDF.createOrReplaceTempView("tblclient")
    spark.sql("select * from tblclient").show()

    jdbcDF
    .write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .save("src/test/resources/jdbc")
    
    jdbcDF
    .write
    .partitionBy("ClientName")
    .format("csv")
    .save("src/test/resources/jdbcpartition")
  }
}