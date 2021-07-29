package com.aron;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //设置用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //开启CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.setRestartStrategy();


        //memory   tm           jm
        //fs       tm           hdfs
        //rocksdb  本地磁盘      hdfs
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ck"));

        //TODO 2.通过FlinkCDC构建Source
        DebeziumSourceFunction<String> stringDebeziumSourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210225-flink")
                .tableList("gmall-210225-flink.base_trademark")//可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())//读表中所有数据，
//                .startupOptions(StartupOptions.earliest())//读表中所有数据及修改数据记录，需要有完整的binlog数据，新开启一个binlog再建表，否则报错
//                .startupOptions(StartupOptions.latest())//只读最新的数据
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(stringDebeziumSourceFunction);

        //TODO 3.打印数据
        dataStreamSource.print();

        //TODO 4.启动任务
        env.execute();
    }
}
