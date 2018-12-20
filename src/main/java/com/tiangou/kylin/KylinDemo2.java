package com.tiangou.kylin;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Maps;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by LZhan
 * Time:9:08
 * Date:2018/12/3
 */
public class KylinDemo2 {
    public static void main(String[] args) {

        DruidDataSource  dataSource=new DruidDataSource();
        dataSource.setUrl(JDBCUtils.KYLIN_URL);
        dataSource.setDriverClassName(JDBCUtils.KYLIN_DRIVER);
        dataSource.setUsername(JDBCUtils.KYLIN_USER);
        dataSource.setPassword(JDBCUtils.KYLIN_PASSWORD);

        JdbcTemplate jdbcTemplate=new JdbcTemplate(dataSource);
        String sql="SELECT COUNT(1),part_dt FROM kylin_sales GROUP BY part_dt ORDER BY part_dt";
        List<Map<String, Object>> result= jdbcTemplate.query(sql, (RowMapper<Map<String, Object>>) (res, index) -> {
            ResultSetMetaData metadata = res.getMetaData();
            int count = metadata.getColumnCount();
            HashMap rowMap = Maps.newHashMap();
            for (int i = 1; i <= count; ++i) {
                rowMap.put(metadata.getColumnName(i), res.getObject(i));
            }
            return rowMap;
        });

        result.stream().forEach(map -> System.out.println(map.get("PART_DT").toString()));




    }

}
