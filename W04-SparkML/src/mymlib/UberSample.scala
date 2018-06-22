package mymlib

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }

object UberSample {

  def main(args: Array[String]): Unit = {
  
     val conf = new SparkConf().setAppName("Uber Data Set Analysiss").setMaster("local[2]")
    val sc = new SparkContext(conf)
   
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val uber_data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/cloudera/git/S-BEAD/W04-SparkML/data/uber.csv")
    println("Uber Data:")
    uber_data.show(5)

    val uberData_new = uber_data.withColumn(
      "BaseNo_Date",
      concat($"dispatching_base_number", lit(":"), $"date"))
    val maxTrips_PerBaseAndDate =
      uberData_new.groupBy("BaseNo_Date").max("trips")
    maxTrips_PerBaseAndDate.show(10)

    // Find the month on which basement has more trips
    val sqlFunc1 = udf(coder1)
    val sqlFunc2 = udf(coder2)
    val uberdata_newMonthCol = uber_data.withColumn(
      "month",
      sqlFunc1(col("date")))
    val uberData_ConcatBaseNo_Month =
      uberdata_newMonthCol.withColumn(
        "BaseNo_Month",
        concat($"dispatching_base_number", lit(":"), $"month"))
    val sumTrips_PerBaseAndMonth =
      uberData_ConcatBaseNo_Month.groupBy("BaseNo_Month").sum("trips")
    val sumTrips_PerBaseMonth_new =
      sumTrips_PerBaseAndMonth.withColumn(
        "BaseNo",
        sqlFunc2(col("BaseNo_Month")))
    val maxTrips_PerBaseMonth =
      sumTrips_PerBaseMonth_new.groupBy("BaseNo").max("sum(trips)")
        .withColumnRenamed("max(sum(trips))", "MaxTrips_PerMonth")

    val maxTrips_Final =
      maxTrips_PerBaseMonth.join(sumTrips_PerBaseMonth_new, sumTrips_PerBaseMonth_new("BaseNo") === maxTrips_PerBaseMonth("BaseNo") && sumTrips_PerBaseMonth_new("sum(trips)") === maxTrips_PerBaseMonth("MaxTrips_PerMonth"))
        .select("BaseNo_Month", "MaxTrips_PerMonth")
    println("Maximum Trips per basement per month:")
    maxTrips_Final.show()
  }
  val coder1 = (dateValue: String) => {
    val format =
      new java.text.SimpleDateFormat("MM/dd/yyyy")
    val formated_Date = format.parse(dateValue)
    formated_Date.getMonth() + 1

  }

  val coder2 = (baseMonthConcat: String) => baseMonthConcat.split(":")

}