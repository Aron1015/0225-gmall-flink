package com.aron.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.aron.app.func.DimAsyncFunction;
import com.aron.bean.OrderDetail;
import com.aron.bean.OrderInfo;
import com.aron.bean.OrderWide;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

//TODO 数据流:web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)     ->       FlinkApp  ->Kafka(dwm)

//TODO 程  序:          mock                -> Mysql  -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(ZK,HDFS,HBase) -> OrderWideApp ->kafka


public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));

        //TODO 2.读取Kafka订单和订单明细主题数据 dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
                //补全字段
                String create_time = orderInfo.getCreate_time();
                String[] datetime = create_time.split(" ");
                orderInfo.setCreate_date(datetime[0]);
                orderInfo.setCreate_hour(datetime[1]);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                return orderInfo;
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSONObject.parseObject(value, OrderDetail.class);
                String create_time = orderDetail.getCreate_time();

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                return orderDetail;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4.双流JOIN
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailDS.keyBy(OrderDetail::getOrder_id);
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS.intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))//设置的时间应该为最大的延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        orderWideDS.print("OrderWide>>>>>>>>>>");

        //TODO 5.关联维度信息

        //5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                        //提取性别字段
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);

                        //提取生日字段
                        String birthday = dimInfo.getString("BIRTHDAY");
                        long curTs = System.currentTimeMillis();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long ts = sdf.parse(birthday).getTime();

                        Long ageLong = (curTs - ts) / (1000L * 60 * 60 * 24 * 365);

                        orderWide.setUser_age(ageLong.intValue());
                    }
                },
                60,
                TimeUnit.SECONDS);
//        orderWideWithUserDS.print("User>>>>>>>>>>>>");
        
        //关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String iso_3166_2 = dimInfo.getString("ISO_3166_2");

                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(iso_3166_2);
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDS.print("province>>>>>>>>>>>>>>>");

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithSkuDS.print("sku>>>>>>>>>>>>>");

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS<<<<<<<<<<<<<<<");

        //TODO 6.将数据写入Kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(orderWideSinkTopic));

        //TODO 7.启动任务
        env.execute();
    }
}
