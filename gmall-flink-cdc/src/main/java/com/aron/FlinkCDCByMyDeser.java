package com.aron;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDCByMyDeser {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.通过FlinkCDC构建Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210225-flink")
                .tableList("gmall-210225-flink.z_user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //TODO 3.打印数据
        dataStreamSource.print();

        //TODO 4.启动任务
        env.execute();
    }

    public static class MyStringDeserializationSchema implements DebeziumDeserializationSchema<String> {

        //{
        // "database":"",
        // "tableName":"",
        // "data":{"id":"1001","tm_name","atguigu"....},
        // "before":{"id":"1001","tm_name","atguigu"....},
        // "type":"update",
        // "ts":141564651515
        // }
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //构建结果对象
            JSONObject jsonObject = new JSONObject();

            //获取数据库名称&表名称
            String[] split = sourceRecord.topic().split("\\.");
            String database = split[0];
            String tableName = split[1];

            //获取数据
            Struct value = (Struct) sourceRecord.value();

            //After
            Struct after = value.getStruct("after");
            JSONObject data = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object filedValue = after.get(field);
                    data.put(field.name(), filedValue);
                }
            }


            //Before
            Struct before = value.getStruct("before");
            JSONObject beforeData = new JSONObject();
            if (before != null) {
                Schema schema = before.schema();
                List<Field> fieldList = schema.fields();

                for (int i = 0; i < fieldList.size(); i++) {
                    Field field = fieldList.get(i);
                    Object filedValue = before.get(field);
                    beforeData.put(field.name(), filedValue);
                }
            }

            //获取操作类型 CREATE UPDATE DELETE

            //封装数据


            //输出封装好的数据
            collector.collect(jsonObject.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }

}
