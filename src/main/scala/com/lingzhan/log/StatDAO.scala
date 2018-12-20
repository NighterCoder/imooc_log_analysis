package com.lingzhan.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by LZhan 
  * Time:16:24
  * Date:2018/11/30
  */
object StatDAO {

  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {

    var conn: Connection = null
    var pstmt: PreparedStatement = null


    try {
      conn = MySQLUtils.getConnection()
      //最好采用批处理，关闭自动提交
      conn.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
      pstmt = conn.prepareStatement(sql)

      //遍历赋参数
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }
      pstmt.executeBatch()
      conn.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }


  }


}
