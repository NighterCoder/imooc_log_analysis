package com.tiangou.kylin;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 创建Kylin连接
 * Created by LZhan
 * Time:9:11
 * Date:2018/12/3
 */
public class JDBCUtils {


     static String KYLIN_URL = PropertyUtil.getProperty("kylin.url");
     static String KYLIN_DRIVER = PropertyUtil.getProperty("kylin.driver");
     static String KYLIN_USER = PropertyUtil.getProperty("kylin.user");
     static String KYLIN_PASSWORD = PropertyUtil.getProperty("kylin.password");

    public static Connection getConnection() {

        Connection conn = null;

        try {

            Driver driver=(Driver)Class.forName(KYLIN_DRIVER).newInstance();
            Properties info=new Properties();
            info.put("user", KYLIN_USER);
            info.put("password", KYLIN_PASSWORD);
            conn=driver.connect(KYLIN_URL,info);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return conn;

    }


    public static void main(String[] args) {
        System.out.println(getConnection());

    }



}

