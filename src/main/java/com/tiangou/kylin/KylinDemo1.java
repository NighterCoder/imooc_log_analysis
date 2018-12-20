package com.tiangou.kylin;

import java.sql.*;

/**
 * Created by LZhan
 * Time:9:08
 * Date:2018/12/3
 */
public class KylinDemo1 {

    public static void main(String[] args) {

        long start=System.currentTimeMillis();

        Connection conn = JDBCUtils.getConnection();
        try {
            if (conn != null) {
                Statement statement = conn.createStatement();

                String sql="SELECT COUNT(1),part_dt FROM kylin_sales GROUP BY part_dt ORDER BY part_dt ";
                ResultSet resultSet=statement.executeQuery(sql);
                while (resultSet.next()){
                    System.out.print(resultSet.getString(1)+"\t");
                    System.out.println(resultSet.getString(2));
                }
                long end=System.currentTimeMillis();
                //耗时
                System.out.println("耗时"+(end-start)/1000+"秒");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }


}
