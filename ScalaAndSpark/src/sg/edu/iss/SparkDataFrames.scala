package sg.edu.iss

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkDataFrames {
   case class Person(name:String, age:Int)
  def main(args:Array[String]): Unit = {

    val conf=new SparkConf
    conf.setMaster("local[2]")
    conf.setAppName("Sample Spark Dataframes")
    val sc=new SparkContext(conf)
    val sqlcontxt=new SQLContext(sc)
    import sqlcontxt.implicits._
    val df = sqlcontxt.read.json("/home/cloudera/workspace/ScalaAndSpark/data/people.json")
      df.show
      df.printSchema
      df.select("name").show
      df.select("name","age").show
      df.select(df("name"),df("age")+4).show
      df.groupBy("age").count.show
      //df.describe("name, age")

  // Load textfile as dataframe and query on the DataFrame
    val peopleDf = sc.textFile("/home/cloudera/workspace/ScalaAndSpark/data/people.txt").
      map(line => line.split(",")).map(p => Person(p(0),p(1).trim.toInt)).toDF


      peopleDf.registerTempTable("people")
      val teenagers = sqlcontxt.sql("select name, age from people where age >=13")
      teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

  
  }
}