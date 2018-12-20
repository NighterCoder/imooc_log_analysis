package com.lingzhan.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 对第一步清洗的日志进一步解析
  * Created by LZhan 
  * Time:11:04
  * Date:2018/11/30
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\resources\\workspace\\tiangou\\scalaProjects\\winutils")


    val spark=SparkSession.builder()
      .appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    //RDD[String]
    val accessRDD=spark.sparkContext
      .textFile("C:/resources/workspace/tiangou/scalaProjects/imooc_log_analysis/src/main/resources/format.log")

    //需要将RDD转化成DF 参数1:RRD[Row] 参数2:StructType   Row的结构类型要与struct一致
    val accessDF=spark.createDataFrame(accessRDD.map(line=>AccessConvertUtil.parseLog(line)),AccessConvertUtil.struct)


//    accessDF.write.format("parquet").partitionBy("day").mode(SaveMode.Overwrite)
//          .save("c:/resources/workspace/tiangou/scalaProjects/clean")

    //合并为一个文件
    accessDF.coalesce(1).write.format("parquet").partitionBy("day").mode(SaveMode.Overwrite)
      .save("c:/resources/workspace/tiangou/scalaProjects/clean1")

    accessDF.printSchema()
    accessDF.show(false)

    spark.stop()
  }
}
