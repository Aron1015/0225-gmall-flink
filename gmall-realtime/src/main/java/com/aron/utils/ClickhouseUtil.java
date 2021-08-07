package com.aron.utils;

import com.aron.bean.TransientSink;
import com.aron.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickhouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //反射获取所有的属性名称
                        Field[] declaredFields = t.getClass().getDeclaredFields();

                        int offset = 0;
                        //遍历字段信息
                        for (int i = 0; i < declaredFields.length; i++) {
                            //获取字段名
                            Field field = declaredFields[i];

                            //设置私有属性值可以访问
                            field.setAccessible(true);

                            //反射获取该字段上的注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);

                            //判断该注解是否存在
                            if (annotation == null) {
                                //获取值
                                Object value = null;
                                try {
                                    value = field.get(t);
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }

                                //给preparedStatement中的？赋值
                                //i代表流对象字段的下标，
                                // 公式：写入表字段位置下标 = 对象流对象字段下标 + 1 - 跳过字段的偏移量
                                // 一旦跳过一个字段 那么写入字段下标就会和原本字段下标存在偏差
                                preparedStatement.setObject(i + 1 - offset, value);
                            } else {
                                offset++;
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        //5条写一次
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
