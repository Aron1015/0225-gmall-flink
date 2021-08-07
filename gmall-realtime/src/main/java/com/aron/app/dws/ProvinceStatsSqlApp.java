package com.aron.app.dws;

import com.aron.bean.ProvinceStats;
import com.aron.utils.ClickhouseUtil;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//TODO 数据流:web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim) 
// -> FlinkApp  -> Kafka(dwm) ->   FlinkApp  ->      ClickHouse

//TODO 程  序:          mock                -> Mysql  -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(ZK,HDFS,HBase) 
// -> OrderWideApp ->kafka -> ProvinceStatsSqlApp -> ClickHouse

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL的方式读取Kafka dwm_order_wide 主题的数据(WaterMark)
        String groupId = "ProvinceStatsSqlApp";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `total_amount` DOUBLE, " +
                "  `create_time` STRING, " +
                "  `rt` AS TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ") " + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId));

        //TODO 3.分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("select  " +
                "  DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "  DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  sum(total_amount) order_amount, " +
                "  count(distinct order_id) order_count, " +
                "  UNIX_TIMESTAMP() AS ts " +
                "from order_wide " +
                "group by  " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  TUMBLE(rt,INTERVAL '10' SECOND)");

        //TODO 4.将动态表转换为数据流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(resultTable, ProvinceStats.class);


        //TODO 5.将数据写入ClickHouse
        provinceStatsDS.print("province>>>>>>>");

//        provinceStatsDS.addSink(ClickhouseUtil.getJdbcSink("insert into "))

        //TODO 6.启动任务
        env.execute();

    }
}
