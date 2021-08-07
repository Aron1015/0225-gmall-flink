package com.aron.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.aron.bean.VisitorStats;
import com.aron.utils.ClickhouseUtil;
import com.aron.utils.DateTimeUtil;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Desc: 访客主题宽表计算
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合
 * 进行关联  这是一个fullJoin
 * 可以考虑使用flinkSql 完成
 */

//TODO 数据流：web/app -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm) -> FlinkApp-> ClickHouse

//TODO 程  序：mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UserJumpDetailApp -> Kafka->VisitorStatsApp->CK
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));
        //TODO 2.读取Kafka 3个主题的数据
        String groupId = "VisitorStatsApp0225";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageViewStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //TODO 3.统一数据格式
        //转换pv流
        SingleOutputStreamOperator<VisitorStats> pageViewDS = pageViewStrDS.map(value -> {
            JSONObject jsonObj = JSONObject.parseObject(value);

            long sv = 0L;
            if (jsonObj.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObj.getJSONObject("page").getLong("during_time"),
                    jsonObj.getLong("ts"));
        });
        //转换uv流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitDS = uniqueVisitStrDS.map(value -> {
            JSONObject jsonObj = JSONObject.parseObject(value);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObj.getLong("ts"));
        });
        //转换sv流
        SingleOutputStreamOperator<VisitorStats> userJumpDS = userJumpStrDS.map(value -> {
            JSONObject jsonObj = JSONObject.parseObject(value);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObj.getLong("ts"));
        });

        //TODO 4.Union
        DataStream<VisitorStats> unionDS = pageViewDS.union(uniqueVisitDS, userJumpDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWM = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组,开窗,聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> visitorStatsWithWindow = visitorStatsWithWM.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new());
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        SingleOutputStreamOperator<VisitorStats> result = visitorStatsWithWindow.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                VisitorStats visitorStats = input.iterator().next();

                String start = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String end = DateTimeUtil.toYMDhms(new Date(window.getEnd()));
                visitorStats.setStt(start);
                visitorStats.setEdt(end);

                out.collect(visitorStats);
            }
        });

        //TODO 7.将数据写入ClickHouse
        result.print();
        result.addSink(ClickhouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute();
    }
}
