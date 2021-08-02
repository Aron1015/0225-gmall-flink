package com.aron.utils;

import com.alibaba.fastjson.JSONObject;
import com.aron.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Phoenix查询的工具类
 */
public class PhoenixUtil {

    //声明
    private static Connection connection;

    //初始化连接
    private static Connection init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

            //设置连接的Phoenix库

            return connection;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean toCamel) {

        //初始化连接
        if (connection == null) {
            connection = init();
        }

        //创建结果集
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {
            //编译sql
            preparedStatement = connection.prepareStatement(querySql);

            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();
            //获取查询结果中元数据信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                //创建泛型对象
                T t = clz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    //取出列名
                    String columnName = metaData.getColumnName(i);

                    //列名是否需要转换,由全大写并_分隔符名称转为首字母小写的驼峰命名
                    if (toCamel) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //获取值
                    String value = resultSet.getString(i);

                    //给对象赋值
                    BeanUtils.setProperty(t, columnName, value);
                }
                //将对象添加至集合
                list.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        //返回结果
        return list;

    }


    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println(queryList(connection, "select * from GMALL210225_REALTIME.DIM_USER_INFO where id = '3'", JSONObject.class, false));

    }
}
