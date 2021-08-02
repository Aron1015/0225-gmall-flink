package com.aron.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.aron.bean.OrderWide;
import com.aron.bean.PaymentInfo;
import com.aron.bean.PaymentWide;
import com.aron.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));

        String paymentInfoTopic = "dwd_payment_info";
        String orderWideTopic = "dwm_order_wide";
        String paymentInfoGroupId = "PaymentWideApp0225";
        String orderWideGroupId = "orderWideApp0225";
        String PaymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoTopic, paymentInfoGroupId));
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideTopic, orderWideGroupId));

        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentStrDS.map(value -> JSONObject.parseObject(value, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @SneakyThrows
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                return sdf.parse(element.getCreate_time()).getTime();
                            }
                        }))
                .keyBy(PaymentInfo::getOrder_id);

        KeyedStream<OrderWide, Long> OrderWideKeyedDS = orderWideStrDS.map(value -> JSONObject.parseObject(value, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @SneakyThrows
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                return sdf.parse(element.getCreate_time()).getTime();
                            }
                        }))
                .keyBy(OrderWide::getOrder_id);

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS.intervalJoin(OrderWideKeyedDS)
                .between(Time.milliseconds(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        paymentWideDS.print("paymentWide>>>>>>>>>");
        
        //写入kafka
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(PaymentWideSinkTopic));
        
        env.execute();


    }
}
