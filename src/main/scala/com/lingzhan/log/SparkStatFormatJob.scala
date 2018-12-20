package com.lingzhan.log

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：选出需要的列
  * Created by LZhan 
  * Time:10:09
  * Date:2018/11/30
  */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("SparkStatFormatJob")
              .master("local[2]").getOrCreate()

    //读取日志文件init.log(一般文件使用textFile)
    val rddLog=spark.sparkContext.
      textFile("C:/resources/workspace/tiangou/scalaProjects/imooc_log_analysis/src/main/resources/init.log")

    //遍历rddLog中的每一行数据并取出对应想要的列
    //需要的列有ip、时间、url、traffic
    rddLog.map(line=>{
      val splits=line.split(" ")
      val ip=splits(0)
      val accessTime=splits(3)+" "+(splits(4))
      val traffic=splits(9)
      val url=splits(10)

      // MM大写区别月和分 HH表示24小时 hh表示12小时
      // 时间格式需要处理 改为yyyy-MM-dd HH:mm:ss

      // (ip,accessTime,traffic,url)
      DateFormatUtils.format(accessTime)+"\t"+url.replaceAll("\"","")+"\t"+traffic+"\t"+ip

    }).take(10).foreach(println)


    spark.stop()


  }

}
