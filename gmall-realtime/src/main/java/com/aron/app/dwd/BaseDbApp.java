package com.aron.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.aron.app.func.DimSinkFunction;
import com.aron.app.func.MyStringDeserializationSchema;
import com.aron.app.func.TableProcessFunction;
import com.aron.bean.TableProcess;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

//TODO 数据流:web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)

//TODO 程  序:          mock                -> Mysql  -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(ZK,HDFS,HBase)

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));

        //TODO 2.读取Kafka ods_base_db 主题数据
        String topic = "ods_base_db";
        String groupId = "BaseDbApp0225";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JSONObject           主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return !"delete".equals(value.getString("type"));
                    }
                });

        //TODO 4.通过FlinkCDC读取配置信息表,并封装为    广播流
        DebeziumSourceFunction<String> tableProcessStrSourceFunc = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-realtime")
                .tableList("gmall-210225-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDeserializationSchema())
                .build();

        //封装广播流
        DataStreamSource<String> tableProcessStrDS = env.addSource(tableProcessStrSourceFunc);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);


        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonObjDS.connect(broadcastStream);
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("HBase") {};
        SingleOutputStreamOperator<JSONObject> process = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor,hbaseTag ));

        //TODO 6.处理连接流数据
        process.print("kafka>>>>>>>>>>>");
        DataStream<JSONObject> HBaseDS = process.getSideOutput(hbaseTag);
        HBaseDS.print("HBase>>>>>>>>>>>");

        //TODO 7.获取Kafka数据流以及HBASE数据流写入对应的存储框架中
        process.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"), jsonObject.getString("data").getBytes());
            }
        }));

        HBaseDS.addSink(new DimSinkFunction());

        //TODO 8.启动任务
        env.execute();
    }
}
