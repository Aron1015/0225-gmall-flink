package com.aron.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.aron.app.func.MyStringDeserializationSchema;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //设置状态后端
//        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.通过FlinkCDC构建Source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDeserializationSchema())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //TODO 3.将数据写入Kafka
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer("ods_base_db"));

        //TODO 4.启动任务
        env.execute();

    }
}
