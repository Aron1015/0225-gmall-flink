package com.aron.app.func;

import com.alibaba.fastjson.JSONObject;
import com.aron.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    //声明phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取插入数据的sql: upsert into db.tn(id,tm_name) values(...)
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));

            System.out.println(upsertSql);

            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入维度数据"+value.getString("data")+"失败!");
        }finally {
            if (null != preparedStatement) preparedStatement.close();
        }
    }

    //upsert into db.tn(id,tm_name) values(...)
    private String genUpsertSql(String sinkTable, JSONObject data) {

        //取出data的字段
        Set<String> columns = data.keySet();

        //取出值
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable
                + "(" + StringUtils.join(columns, ",") + ") values('"
                + StringUtils.join(values, "','") + "')";
    }
}
