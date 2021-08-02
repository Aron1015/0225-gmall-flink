package com.aron.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//TODO 数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)

//TODO 程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));
        //TODO 2.读取Kafka  dwd_page_log  主题数据
        String groupId = "unique_visit_app210225";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将数据转换为JSONObject
        KeyedStream<JSONObject, String> keyedDS = kafkaDS.map(JSONObject::parseObject)
                //TODO 4.按照Mid分组
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程对数据进行按天去重过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> visitDateState;
            private SimpleDateFormat sdf;


            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("visit_state", String.class);
                //状态可以设置时间，到期清除
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        //设置当状态被创建和写入时重置时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                visitDateState = getRuntimeContext().getState(stateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一跳页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null) {

                    //取出状态数据
                    String visitDate = visitDateState.value();
                    String curDate = sdf.format(value.getLong("ts"));

                    if (visitDate == null || !visitDate.equals(curDate)) {
                        //将当前日期写入状态
                        visitDateState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //TODO 6.将数据写入Kafka
        filterDS.print();

        filterDS.map(value -> JSONObject.toJSONString(value))
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 7.启动任务
        env.execute();
    }
}
