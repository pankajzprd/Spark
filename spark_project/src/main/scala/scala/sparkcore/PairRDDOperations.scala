package scalaclasses.sessions

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class Event(organizer: String, name: String, budget: Int)

object PairRDDOperations {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder
      .appName("mapExample")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("datasets/events.txt")

    // val rdd1 = spark.sparkContext.broadcast(rdd)
    //rdd.cache()
    //  rdd.persist(StorageLevel.MEMORY_ONLY)
    val mapfunc = rdd.map(
      stringArg => {
        val array = stringArg.split(",")
        new Event(array(0), array(1), array(2).toInt)
      })

    /* Create a Pair RDD */
    println("Eg. 1) Create a Pair RDD")
    // create a pairRdd from rdd by pairing the organizer with the event budget
    val pairRdd = mapfunc.map(event => (event.organizer, event.budget)) // Pair RDD
    pairRdd.collect().foreach(println)

    /* Group the Pair RDD using orgainzer as the key */
    println("Eg. 2) Group the Pair RDD using orgainzer as the key")
    val groupedRdd = pairRdd.groupByKey()
    groupedRdd.collect().foreach(println)

    /* Instead of grouping, reduce the pair rdd to organizer with the total budget */
    println("Eg. 3) Instead of grouping, reduce the pair rdd to organizer with the total budget")
    val reducedRdd = pairRdd.reduceByKey(_ + _)
    reducedRdd.collect().foreach(println)

    /* Instead of just reducing to organizer with the total of the budget, reduce
   * to organizer and avg. budget */
    println("Eg. 4) Instead of just reducing to organizer with the total of the budget, reduce to organizer and avg. budget")
    val coupledValuesRdd = pairRdd.mapValues(v => (v, 1))
    println("\ncoupledValuesRdd: ")
    coupledValuesRdd.collect().foreach(println)

    // since the value is a pair at this point, it will also return a pair as a value
    // This results in (organizer, (total budget, total no.of events) )
    val intermediate = coupledValuesRdd.reduceByKey((v1, v2) => ((v1._1 + v2._1), (v1._2 + v2._2)))
    println("\n intermediate : ")
    intermediate.collect().foreach(println)

    val averagesRdd = intermediate.mapValues(pair => pair._1 / pair._2)
    println("\n averagesRdd : ")
    averagesRdd.collect().foreach(println)

  }

}