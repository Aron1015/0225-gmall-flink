package com.aron.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//TODO 数据流： app/web -> nginx -> springboot -> kafka(ods) -> flinkApp -> kafka(dwd)

//TODO 程序：   数据生成脚本 -> nginx ->logger -> kafka(zk) -> BaseLogApp -> kafka

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ck"));

        //TODO 2.读取Kafka ods_base_log 主题数据
        String topic = "ods_base_log";
        String groupId = "BaseLogApp0225";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将数据转换为JSONObject
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.map( value -> JSONObject.parseObject(value));
        //考虑到脏数据可能导致运行报错停止，需要考虑，更改后如下
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    out.collect(JSONObject.parseObject(value));
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("DirtyData") {
                    }, value);
                }
            }
        });
        //获取脏数据并打印,也可以保存到路径上
        jsonObjDS.getSideOutput(new OutputTag<String>("DirtyData") {
        }).print("Dirty>>>>>>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 5.新老用户校验(状态编程)
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //定义状态
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("flag_state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {
                    if (valueState.value() != null) {
                        //将数据中的1改成0
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //将"0"写入状态
                        valueState.update("0");
                    }
                }
                return value;

            }
        });

        //TODO 6.使用侧输出流对数据进行分流处理  页面-主流  启动-侧输出流  曝光-侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取启动数据
                String start = value.getString("start");

                //判断是否为启动数据
                if (start != null && start.length() > 0) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //不是启动数据就是页面数据

                    //页面数据输出主流
                    out.collect(value.toJSONString());

                    //获取曝光数据
                    JSONArray display = value.getJSONArray("displays");//注意displays

                    //判断是否存在曝光数据
                    if (display != null && display.size() > 0) {

                        String pageId = value.getJSONObject("page").getString("page_id");
                        //遍历曝光数据并写出到display侧输出流
                        for (int i = 0; i < display.size(); i++) {
                            //取出单条曝光数据
                            JSONObject jsonObject = display.getJSONObject(i);

                            //添加页面id
                            jsonObject.put("page_id",pageId);

                            //输出到曝光侧输出流
                            ctx.output(displayTag, jsonObject.toJSONString());

                        }
                    }
                }
            }
        });

        //TODO 7.获取所有流的数据并将数据写入Kafka对应的主题
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        startDS.print("start>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>");
        pageDS.print("page>>>>>>>>>>>>>>");

        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";
        String pageTopic = "dwd_page_log";

        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(startTopic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(displayTopic));
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(pageTopic));


        //TODO 8.启动任务
        env.execute();

    }
}
