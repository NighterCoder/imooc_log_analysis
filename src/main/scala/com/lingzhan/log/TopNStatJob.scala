package com.lingzhan.log

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业
  * Created by LZhan 
  * Time:9:56
  * Date:2018/11/30
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()


    val accessDF = spark.read.format("parquet").load("c:/resources/workspace/tiangou/scalaProjects/clean1")

    //    accessDF.printSchema()
    //    accessDF.show(false)

    //val day="20170511"

    //最受欢迎的TopN课程
    // videoAccessTopNStat(spark, accessDF)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF)


    //按照流量进行统计


    spark.stop()

  }


  /**
    * 最受欢迎的TopN课程
    * 使用DataFrame API统计
    *
    * @param spark
    * @param df
    */
  def videoAccessTopNStat(spark: SparkSession, df: DataFrame): Unit = {

    //1.选择cmsType为video的row
    //    df.filter(df.col("cmsType").equalTo("video")&&df.col("day").equalTo("20170511"))
    //      .groupBy(df.col("cmsId"),df.col("day"))

    val videoAccessTopNDF = df.filter(df.col("cmsType") === "video" && df.col("day") === "20170511")
      .groupBy(df.col("cmsId"), df.col("day"))
      .agg(count(df.col("cmsId")).as("times"))
      .orderBy("times")

    //println(df.filter(df.col("cmsType") === "video" && df.col("day")==="20170511" ).count())   //2507
    //videoAccessTopNDF.agg(sum($"times")).show()     //2507
    videoAccessTopNDF.show()
    // 使用$ ,需要加上隐式转化 import spark.implicits._
    // df.filter($"cmsType" === "video"&&$"day" === "20170511")

    /**
      * 将统计结果写入到MySQL中
      */

    //foreachPartition:action操作，一般落地数据到数据存储系统中时使用
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {

        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(line => {
          val day = line.getAs[String]("day")
          val cmsId = line.getAs[Long]("cmsId")
          val times = line.getAs[Long]("times")

          //ListBuffer添加元素是append
          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })

    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  /**
    * 按照地市进行统计TopN课程
    *
    * @param spark
    * @param df
    */
  def cityAccessTopNStat(spark: SparkSession, df: DataFrame) = {

    import spark.implicits._

    val cityAccessTopNStatDF=df.filter($"cmsType" === "video" && $"day" === "20170511")
      .groupBy($"cmsId", $"day", $"city")
      .agg(count($"cmsId").as("times"))
      .orderBy("city","times")

    //窗口函数在spark sql中的应用




    cityAccessTopNStatDF.show(false)

  }



}
