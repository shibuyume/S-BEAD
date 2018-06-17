package sg.edu.iss

import org.apache.hadoop.io.{Text, IntWritable}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Loading JSON file
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

import java.io.{StringWriter, StringReader}
import au.com.bytecode.opencsv.{CSVWriter, CSVReader}
import scala.collection.JavaConverters._
 import collection.JavaConverters._

object SparkLoadAndSave {
  
   case class Person(name:String, age:Int)

  case class Stocks(name:String, totalPrice:Long)

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf
    conf.setMaster("local[2]").setAppName("SparkLoadingAndSavingData")
    val sc = new SparkContext(conf)

   val mapper = new ObjectMapper()
    // Loading text file
    val input =
      sc.textFile("/home/cloudera/workspace/ScalaAndSpark/data/README.TXT")

    input.saveAsTextFile("/home/cloudera/workspace/ScalaAndSpark/data/outputFileWholeInput")

 // Loading JSON File
    val jsonInput = sc.textFile("/home/cloudera/workspace/ScalaAndSpark/data/people.json")
      val result1 = jsonInput.flatMap(record => {
      try{Some(mapper.readValue(record, classOf[Person]))
      }
      catch{
      case e:Exception => None
      }} )
      result1.filter(person => person.age>15).map(mapper.writeValueAsString(_)).
      saveAsTextFile("/home/cloudera/workspace/ScalaAndSpark/data/outputFile")



    // Loading CSV

    val input1 = sc.textFile("/home/cloudera/workspace/ScalaAndSpark/data/stocks.csv")
    val result2 = input1.flatMap{line => val reader = new CSVReader(new
        StringReader(line))
      reader.readAll().asScala.toList.map(x => Stocks(x(0), x(5).toLong))
    }

    result2.foreach(println)
    result2.map(stock => Array((stock.name, stock.totalPrice))).mapPartitions {stock =>
      val stringWriter = new StringWriter
      val csvWriter = new CSVWriter(stringWriter)

      csvWriter.writeAll(stock.toList.map(arr => arr.map(x => x._1+x._2.toString)).asJava)
      Iterator(stringWriter.toString)
    }.saveAsTextFile("/home/cloudera/workspace/ScalaAndSpark/data/CSVOutputFile")

    //Loading Sequence File
    
    val data = sc.sequenceFile("/sequenceFile/path", classOf[Text],
      classOf[IntWritable]).map{case(x,y) => (x.toString, y.get())}
    val input3 = sc.parallelize(List(("Panda",3),("Kay",6),
      ("Snail",2)))
    input3.saveAsSequenceFile("hdfs://quickstart.cloudera/user/cloudera/sequenceOutputFile")
  }

  
}