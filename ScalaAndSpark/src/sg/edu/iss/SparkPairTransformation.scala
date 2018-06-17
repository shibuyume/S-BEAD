package sg.edu.iss

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkPairTransformation {
  
  def main(args:Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local[2]").setAppName("SparkPairRDDTransformations")
    val sc = new SparkContext(conf)

    val baseRdd =
      sc.parallelize(Array("this,is,a,classroom","it,is,a,monitor","john,is, in,town,hall"))
        val inputRdd = sc.makeRDD(List(("is",2), ("it",2), ("classroom",8), ("this",6),("john",5),("a",1)))
        val wordsRdd = baseRdd.flatMap(record => record.split(","))
        val wordPairs = wordsRdd.map(word => (word, word.length))
        val filteredWordPairs = wordPairs.filter{case(word, length) =>
        length >=2}

    val textFile = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/stocks.txt")
      val stocksPairRdd = textFile.map{record => val colData =
      record.split(",")
      (colData(0),colData(6))}

    val stocksGroupedRdd = stocksPairRdd.groupByKey
    val stocksReducedRdd = stocksPairRdd.reduceByKey((x,y)=>x+y)
    val subtractedRdd = wordPairs.subtractByKey(inputRdd)
    val cogroupedRdd = wordPairs.cogroup(inputRdd)
    val joinedRdd = filteredWordPairs.join(inputRdd)
    val sortedRdd = wordPairs.sortByKey(true)
    println("Sorted RDD")
    println(sortedRdd)
    val leftOuterJoinRdd = inputRdd.leftOuterJoin(filteredWordPairs)
    val rightOuterJoinRdd = wordPairs.rightOuterJoin(inputRdd)
    val flatMapValuesRdd = filteredWordPairs.flatMapValues(length =>
      1 to 5)
    val mapValuesRdd = wordPairs.mapValues(length => length*2)
    val keys = wordPairs.keys
    val values = filteredWordPairs.values
    println("Values")
    println(values)
  }
}