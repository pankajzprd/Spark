package scala.sparksql
import org.apache.spark.sql.SparkSession
object sparkdatasetjoins {
  case class employee(Id: Integer, Name: String, Salary: Float)
  case class profession(Id: Integer, Did: Integer, Profession: String)
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
      .as[employee]

    val professions = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/test/resources/datasets/employee_professions.csv")
      .as[profession]

    println("Joins on Datasets")
    val innerds = employees.joinWith(professions, employees("Id") === professions("Id"), "inner")
    innerds.show()
    val leftds = employees.joinWith(professions, employees("Id") === professions("Id"), "left_outer")
    leftds.show()
    val rightds = employees.joinWith(professions, employees("Id") === professions("Id"), "right_outer")
    rightds.show()

    println("Using Aliases")
    val innerds1 = employees.as("e").joinWith(professions.as("p"), $"e.Id" === $"p.Id", "inner")
    innerds1.show()

    println("Selecting few  columns")
    innerds1.select($"_1.Id", $"_1.Name", $"_1.Salary", $"_2.Profession").show
    leftds.select($"_1.Id", $"_2.profession").show()

    println("Converting Datasets into Temporary tables")
    employees.createOrReplaceTempView("emp")
    professions.createOrReplaceTempView("prof")

    println("Temporary View Dataset Inner Join")
    val innerjoinds = spark.sql("Select emp.Id, emp.name, emp.salary, prof.profession from emp inner join prof on emp.id = prof.id ").show()
    println()
    println("Temporary View Dataset Left Outer Join")
    val leftjoinds = spark.sql("Select emp.Id, emp.name, emp.salary, prof.profession from emp left outer join prof on emp.id = prof.id ").show()
    println()
    println("Temporary View Dataset Right Outer Join")
    val rightjoinds = spark.sql("Select emp.Id, emp.name, emp.salary, prof.profession from emp right outer join prof on emp.id = prof.id ").show()

    //employees.show()
    //professions.show()
   // employees.printSchema()
   // professions.printSchema()

  }
}