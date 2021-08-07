package com.aron.app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//TODO 数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)

//TODO 程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UserJumpDetailApp -> Kafka

public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.读取Kafka dwd_page_log 主题数据
        String topic = "dwd_page_log";
        String groupId = "UserJumpDetailApp0225";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaStreamDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.转换为JSONObject并分组
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaStreamDS.map(JSONObject::parseObject);

        //添加waterMark信息，指定数据中的ts字段为事件时间
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //因为用户的行为都是要基于相同的mid的行为判断，所以要根据mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 4.定义模式序列
        //筛选条件为：1.last_page_id为空，2.并且下条数据的last_page_id也为空，为条件筛选出一次会话;
        // 第二个条件可能会超时，结果也算在内，因为这种情况是因为该设备仅产生了一次会话
        Pattern<JSONObject, JSONObject> pattern1 = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10L));
        //方式二
        Pattern<JSONObject, JSONObject> pattern2 = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)//简写
                .consecutive() //当使用简写，与上一行搭配，严格近邻
                .within(Time.seconds(10L));


        //TODO 5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern1);

        //TODO 6.提取事件(包含匹配上的以及超时事件)
        //创建侧输出流保存超时事件，代表该用户仅单跳访问一次，也算入结果
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeout") {};

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });

        //TODO 7.获取侧输出流数据并与主流进行Union
        DataStream<JSONObject> sideOutput = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(sideOutput);

        //TODO 8.将数据写入Kafka
        unionDS.print();
        unionDS.map(jSONObject->JSONObject.toJSONString(jSONObject))
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        //TODO 9.启动任务
        env.execute();

    }

}
