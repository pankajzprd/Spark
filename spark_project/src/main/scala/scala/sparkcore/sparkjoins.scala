package sparkcore
import org.apache.spark.sql.SparkSession
/*
 * Resources
 * https://stackoverflow.com/questions/44348984/join-2-rdds-using-scala
 * https://github.com/rohgar/scala-spark-4/wiki/Pair-RDDs:-Joins
 * http://apachesparkbook.blogspot.com/2015/12/join-leftouterjoin-rightouterjoin.html
 */
object sparkjoins {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession.builder()
      .appName("Spark Joins")
      .master("local[*]")
      .getOrCreate()

    /*
     * Joins in Scala
     * We can join two collections, input files in Scala and produce results from it.
     * In order to Join, we must have a (key, value) pair.
     * The join will occur based on Key from both the collections/inputs
     */
    println("Joins with Collections")
    val input1 = Seq(("Math", 81), ("English", 67), ("Chem", 81), ("Phy", 98), ("Sanskrit", 78))
    val input2 = Seq(("Biology", 85), ("English", 67), ("Chem", 81), ("Phy", 98), ("Urdu", 78))
    val rdd1 = spark.sparkContext.parallelize(input1, 2)
    val rdd2 = spark.sparkContext.parallelize(input2, 3)

    println("Inner Join")
    val innerjoin = rdd1.join(rdd2, 2)
    innerjoin.collect().foreach(println)
    innerjoin.take(10)
    println("Left Join")
    val leftouterjoin = rdd1.leftOuterJoin(rdd2, 3)
    leftouterjoin.collect().foreach(println)
    println("Right Join")
    val rightouterjoin = rdd1.rightOuterJoin(rdd2, 4)
    rightouterjoin.collect().foreach(println)

    /*
     * Let us work on input files to work on joins
     * If we use sparkContext.textFile API, we get RDD from our input files
     * We can not directly perform joins in RDDs
     * We need to convert the RDDs into (Key,Value) pairs first and then the join
     * To convert to Key-Value pairs, we'll use map function on the RDDs
     */
    println("Joins with Input files")
    val empfile = spark.sparkContext.textFile("src/test/resources/datasets/employee.txt", 2)
    // Our file data is separated by "," and hence we are taking care of it in the below lines(45, 47)
    val employee = empfile.map(line => line.split(","))
    val emp_prof_file = spark.sparkContext.textFile("src/test/resources/datasets/employee_professions.txt", 2)
    val emp_prof = emp_prof_file.map(line => line.split(","))

    val emp_rdd = employee.map(x => (x(0).toInt, x.mkString(","))) // Getting it Emp_Id as key and rest as values
    val empprof_rdd = emp_prof.map(y => (y(0).toInt, y.mkString(","))) //Getting it Emp_Id as key and rest as values
    println("Inner Join on Files")
    val innerjoinrdd = emp_rdd.join(empprof_rdd)
    innerjoinrdd.collect().foreach(println)
    println("Left Outer Join on Files")
    val leftouterjoinrdd = emp_rdd.join(empprof_rdd, 2)
    leftouterjoinrdd.collect().foreach(println)
    println("Right Outer Join on Files")
    val rightouterjoinrdd = emp_rdd.join(empprof_rdd)
    rightouterjoinrdd.collect().foreach(println)

    /*
     * Joins on RDDs can also be made more sophisticated with the help of CASE CLASS
     * We can define CASE CLASS to match the values/schema of the input files
     */
    case class employees(emp_id: Int, name: String, salary: Float)
    case class employee_profession(emp_id: Int, dept_id: Int, profession: String)

    /*
   val empfile = spark.sparkContext.textFile("datasets/employee.txt", 2)
   Our file data is separated by "," and hence we are taking care of it in the below lines(45, 47)
   val employee = empfile.map(line => line.split(","))
    val emp_prof_file = spark.sparkContext.textFile("datasets/employee_professions.txt", 2)
   val emp_prof = emp_prof_file.map(line => line.split(","))
   */
    println("Joining files with Case class")
    val emp_case_rdd = employee.map(x => (x(0).toInt, employees(x(0).toInt, x(1), x(2).toFloat)))
    val emp_prof_case_rdd = emp_prof.map(y => (y(0).toInt, employee_profession(y(0).toInt, y(1).toInt, y(2))))
    println("Inner join with CASE CLASS")
    val innerjoincase = emp_case_rdd.join(emp_prof_case_rdd, 2)
    innerjoincase.collect().foreach(println)
    println("Left Outer Join with CASE CLASS")
    val leftoutercase = emp_case_rdd.join(emp_prof_case_rdd, 3)
    leftoutercase.collect().foreach(println)
    println("Right Outer Join with CASE CLASS")
    val rightoutercase = emp_case_rdd.join(emp_prof_case_rdd, 2)
    rightoutercase.collect().foreach(println)

    println("Selecting few columns")
    val  innerjoinfewcolumns = innerjoincase.map(x => ( x._2._1.emp_id, x._2._1.name, x._2._2.profession, x._2._1.salary))
    .collect()
    .foreach(println)
    
    val leftouterjoinfewcolumns = rightoutercase.map(y => (y._2._1.emp_id, y._2._2.profession))
    .collect()
    .foreach(println)
  }
  
}