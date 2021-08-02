package com.aron.app.func;

import com.alibaba.fastjson.JSONObject;
import com.aron.bean.TableProcess;
import com.aron.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //定义属性,状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //定义属性,侧输出流标记
    private OutputTag<JSONObject> hbaseTag;

    //声明Phoenix连接
    private Connection connection;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //数据样式： {"databases":"","tableName":"","data":{"sourceTable":"","operateType":""..},"before":{},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(value);
        //将data数据取出
        String data = jsonObject.getString("data");
        //转为javaBean
        TableProcess tableProcess = JSONObject.parseObject(data, TableProcess.class);

        //检验表是否存在，如果不存在则需要在Phoenix中建表
        String sinkType = tableProcess.getSinkType();
        String type = jsonObject.getString("type");
        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
        //写入状态

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    //建表语句 create table if not exists db.table(id varchar primary key,tm_name varchar) ...
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //处理字段
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        try {
            //获取建表语句
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                } else {
                    createTableSql.append(column).append(" varchar");
                }
                //判断不是最后一个字段
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")")
                    .append(sinkExtend);

            System.out.println(createTableSql);

            //预编译SQL并赋值
            PreparedStatement preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行SQL语句并提交
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表" + sinkTable + "失败！");
        }

    }

    //value:{"database":"","tableName":"","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            //2.过滤字段,保留广播流配置里的指定字段
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.分流
            String sinkType = tableProcess.getSinkType();
            //将输出表或者主题信息放入数据继续向下游传输
            value.put("sinkTable", tableProcess.getSinkTable());//需要在主流添加sinkTable信息方便后续往Phoenix插入数据使用

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //将数据写入侧输出流
                ctx.output(hbaseTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //将数据写入主流
                out.collect(value);
            }
        } else {
            System.out.println(key+"不存在!");
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> column = Arrays.asList(columns);

        //对JSONObject 进行迭代器遍历，将不等于广播流的字段去掉
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!column.contains(next.getKey())) {
                iterator.remove();
            }
        }
        //简写
//        data.entrySet().removeIf(next -> !column.contains(next.getKey()));
    }
}
