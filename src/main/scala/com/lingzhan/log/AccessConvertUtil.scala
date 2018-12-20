package com.lingzhan.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换
  * Created by LZhan 
  * Time:11:10
  * Date:2018/11/30
  */
object AccessConvertUtil {


  //格式
  val struct=StructType{
    Array(
      StructField("url",StringType,false),
      StructField("cmsType",StringType,false),
      StructField("cmsId",LongType,false),
      StructField("traffic",LongType,false),
      StructField("ip",StringType,false),
      StructField("city",StringType,false),
      StructField("time",StringType,false),
      StructField("day",StringType,false)
    )
  }


  val domain=s"http://www.imooc.com/"


  /**
    * 根据输入的每一行信息转换成输出的样式,以Row的形式定义
    * @param log
    */
  def parseLog(log:String)={

    try{
      val splits=log.split("\t")
      val time=splits(0)
      val url=splits(1)
      val traffic=splits(2).toLong
      val ip=splits(3)

      //处理split(1)
      val cms=splits(1).substring(url.indexOf(domain)+domain.length)
      var cmsType=""
      var cmsId=0L
      //需要判断cms的长度
      if(cms.length()>1){
        cmsType=cms.split("/")(0)
        cmsId=cms.split("/")(1).toLong
      }
      val city=IpUtils.getCity(ip)
      val day=time.split(" ")(0).replaceAll("-","")

      Row(url,cmsType,cmsId,traffic,ip,city,time,day)

    }catch {
      case e:Exception=>Row(0)
    }

  }



}
