package scalaclasses.sessions

import org.apache.spark.sql.SparkSession
/*
 * groupByKey - When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable) pairs.
 *  By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
 *
 */
object groupByReduceByCombineByKey_1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Comparing between GroupBy, ReduceBy and CombineByKey")
      .master("local[*]")
      .getOrCreate()

    val football = spark.sparkContext.textFile("datasets/football.txt")

    val Tuplefoot = football.map { line =>
      (
        line.split(" ")(0),
        line.split(" ")(1).toInt)
    }
    //    Tuplefoot.collect().foreach(println)
    // groupByKey
    Tuplefoot.groupByKey().foreach(println)

    // Finding totalgoals by Each Player
    /*
     * When a groupByKey is called on a RDD pair the data in the partitions are shuffled over the network to form a key and list of values.
     * This is a costly operation particularly when working on large data set.
     * This might also cause trouble when the combined value list is huge to occupy in one partition. In this case a disk spill will occur.
     */
    val totalgoalsgroup = Tuplefoot.groupByKey().map { line => (line._1, line._2.sum) }
    totalgoalsgroup.foreach(println)

    /*
     * reduceByKey(function) - When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
     * where the values for each key are aggregated using the given reduce function.
     * The function should be able to take arguments of some type and it returns same result data type.
     * Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
     */
    val totalgoalsreduce = Tuplefoot.reduceByKey { case (a, b) => a + b }
    totalgoalsreduce.foreach(println)

    /*
    * combineByKey is most general per-key aggregation function.
    * In fact other aggregate functions described above are derived from combineByKey.
    * This makes combineByKey little bit tricky to comprehend.
    */
    val totalgoalscombine = Tuplefoot
      .combineByKey(
        (comb: Int) => (comb), (a: Int, comb: Int) => (a + comb), (a: Int, b: Int) => (a + b))
    totalgoalscombine.foreach(println)

    // Another way
    Tuplefoot.combineByKey(
      (comb: Int) => {
        println(s"""createCombiner is going to create first combiner for ${comb}""")
        (comb)
      }, (a: Int, comb: Int) => {
        println(s"""mergeValue is going to merge ${a}+${comb} values in a single partition""")
        (a + comb)
      }, (a: Int, b: Int) => {
        println(s"""mergeCombiner is going to merge ${a}+${b} combiners across partition""")
        (a + b)
      }).foreach(println)

  }
}