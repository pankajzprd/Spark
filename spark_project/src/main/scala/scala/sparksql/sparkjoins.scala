package sparksql

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
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
    val log = sc.setLogLevel("WARN")
    //  implicits are important to work on Spark SQL
    import spark.implicits._

    // If you read in below manner, we'll get RDDs
    val emp = sc.textFile("src/test/resources/datasets/employee.txt", 2).map(x => x.split(","))
    val prof = sc.textFile("src/test/resources/datasets/employee_professions.txt", 2).map(y => y.split(","))

    val empdf = emp.map(x => empl(x(0).toInt, x(1), x(2).toFloat)).toDF()
    val profdf = prof.map(y => profl(y(0).toInt, y(1).toInt, y(2))).toDF()

    println("DataFrame Inner Join")
    val innerdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "inner")
    innerdf.collect().foreach(println) // The output is shown as collection of elements here and not as table
    innerdf.show() // The Output will be in table format
    println("DataFrame Left Outer Join")
    val leftdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "left_outer")
    leftdf.collect().foreach(println)
    leftdf.show()
    println("DataFrame Right Outer Join")
    val rightdf = empdf.as("a").join(profdf.as("b"), $"a.emp_id" === $"b.emp_id", "right_outer")
    rightdf.collect.foreach(println)
    rightdf.show()

    println("Selecting few columns")
    innerdf.select($"a.emp_id", $"a.name", $"a.salary", $"b.profession").show()
    println()
    /*
     * We can create temporary views on the DataFrames and perform our joins with simple SQL syntax
     */

    println("Joins using Temporary tables")
    empdf.createOrReplaceTempView("employee")
    profdf.createOrReplaceTempView("Professions")

    println("DataFrame Inner Join with Temporary View")
    val innerjoins = spark.sql("Select * from employee inner join professions  on employee.emp_id = professions.emp_id")
    innerjoins.show()
    println()
    println("DataFrame Left Outer Join with Temporary View")
    val leftouterjoins = spark.sql("select * from employee left outer join professions on employee.emp_id = professions.emp_id")
    leftouterjoins.show()
    println()
    println("DataFrame Right Outer Join with Temporary View")
    val rightouterjoins = spark.sql("select a.emp_id, a.name, a.salary, b.dept_id, b.profession from employee a right outer join professions b on a.emp_id = b.emp_id")
    rightouterjoins.show()
    /*
     * While storing the output of the joins, make sure there are no duplicates
     * "repartition(1)" enables to create single csv file at the output folder.
     * If we don't specify this, we'll end up with multiple files at output location
     */
    rightouterjoins
      .repartition(1)
      .write
      .format("csv")
      .save("src/test/resources/rightouter")
    //If you read in below manner, we'll get DataFrames
    /*   println("Joins on DataSets")
    val empfile = spark.read.text("src/test/resources/datasets/employee.txt").as[empl]
    val proffile = spark.read.text("src/test/resources/datasets/employee_professions.txt").as[profl]
    empfile.collect().foreach(println)
    val innerds = empfile.as("e").join(proffile.as("p"), $"e.emp_id" === $"p.emp_id", "inner")
    val leftds = empfile.as("e").join(proffile.as("p"), $"e.emp_id" === $"p.emp_id", "left_outer")
    val rightds = empfile.as("e").join(proffile.as("p"), $"e.emp_id" === $"p.emp_id", "right_outer")
*/
    //   innerds.select($"e.emp_id", $"e.name", $"e.salary", $"p.profession").show
  }
}