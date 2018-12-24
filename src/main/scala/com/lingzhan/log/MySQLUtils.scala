package com.lingzhan.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Mysql操作工具类
  * Created by LZhan 
  * Time:15:58
  * Date:2018/11/30
  */
object MySQLUtils {


  /**
    * 获取数据库连接
    */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc?characterEncoding=utf8","root","")
  }


  /**
    * 释放数据库连接
    * @param connection
    * @param pstmt
    */
  def release(connection:Connection,pstmt:PreparedStatement):Unit={
    try{
      if(pstmt!=null){
        pstmt.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if (connection!=null){
        connection.close()
      }
    }
  }



  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
