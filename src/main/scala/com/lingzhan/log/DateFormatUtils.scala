package com.lingzhan.log

import java.util.Locale

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by LZhan 
  * Time:10:29
  * Date:2018/11/30
  */
object DateFormatUtils {

  val SOURCE_FORMAT = "MM/dd/yyyy:HH:mm:ss Z"
  val RESULT_FORMAT = "yyyy-MM-dd HH:mm:ss"
  //创建日期格式化对象
  //从字符串到指定的格式日期，使用日期格式化对象的parse方法
  val source_format = FastDateFormat.getInstance(SOURCE_FORMAT,Locale.ENGLISH)
  val result_format = FastDateFormat.getInstance(RESULT_FORMAT)

  def format(source: String): String = {
    result_format.format(getTime(source))

  }

  /**
    * 将日志中字符串日志转换成Date类型，以供转变日期格式(也可以转成Long类型)
    * @param time
    * @return
    */
  def getTime(time:String):Long={
    //日期转换需要catch
    try {
      source_format.parse(time.substring(1,time.length-1)).getTime
    }catch {
      case e:Exception=>{
        0L
      }
    }
  }


  def main(args: Array[String]): Unit = {
    println(format("[11/05/2017:00:01:02 +0800]"))
  }

}
