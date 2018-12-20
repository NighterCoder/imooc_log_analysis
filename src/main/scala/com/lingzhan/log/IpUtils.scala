package com.lingzhan.log

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  * Created by LZhan 
  * Time:9:03
  * Date:2018/11/30
  */
object IpUtils {

  def getCity(ip:String):String={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
  }


}
