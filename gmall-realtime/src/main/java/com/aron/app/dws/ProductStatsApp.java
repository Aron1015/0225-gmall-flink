package com.aron.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aron.app.func.DimAsyncFunction;
import com.aron.bean.OrderWide;
import com.aron.bean.PaymentWide;
import com.aron.bean.ProductStats;
import com.aron.common.GmallConstant;
import com.aron.utils.ClickhouseUtil;
import com.aron.utils.DateTimeUtil;
import com.aron.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ack"));
        //TODO 2.读取Kafka数据
        String groupId = "ProductStatsApp210225";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.统一数据格式
        //3.1 整理pageViewDStream信息
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(value);
                //取出数据中的ts
                Long ts = jsonObject.getLong("ts");

                //取出页面信息
                JSONObject jsonObjectPage = jsonObject.getJSONObject("page");
                String item_type = jsonObjectPage.getString("item_type");
                String page_id = jsonObjectPage.getString("page_id");
                if ("sku_id".equals(item_type) && "good_detail".equals(page_id)) {
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(jsonObjectPage.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();

                    out.collect(productStats);
                }

                //取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build();

                            out.collect(productStats);
                        }
                    }
                }
            }
        });

        //3.2 整理收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.3 加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.4 下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderWideDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                OrderWide orderWide = JSONObject.parseObject(value, OrderWide.class);

                //创建set集合，存放所有的orderId，由于其可以去重，所以最后取该集合长度，就是该商品所有的下单数
                HashSet<Long> orderIds = new HashSet<>();
                orderIds.add(orderWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getTotal_amount())
                        .orderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                        .build();
            }
        });

        //3.5 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                PaymentWide paymentWide = JSONObject.parseObject(value, PaymentWide.class);

                //创建set集合，存放所有的orderId，由于其可以去重，所以最后取该集合长度，就是该商品所有的支付数
                HashSet<Long> orderIds = new HashSet<>();
                orderIds.add(paymentWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getTotal_amount())
                        .paidOrderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        //3.6 退款数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                HashSet<Long> orderIds = new HashSet<>();
                orderIds.add(jsonObject.getLong("order_id"));

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(orderIds)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();

            }
        });

        //3.7 评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                String appraise = jsonObject.getString("appraise");
                long goodCt = 0;
                if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                    goodCt = 1;
                }

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();

            }
        });

        //TODO 4.Union
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderWideDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.分组、开窗&聚合
        //取ts字段创建waterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        SingleOutputStreamOperator<ProductStats> productStatsWithSkuKeyDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getOrderIdSet());

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        long stt = window.getStart();
                        long edt = window.getEnd();

                        ProductStats productStats = input.iterator().next();
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(stt)));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(edt)));

                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        out.collect(productStats);
                    }
                });

        //TODO 6.关联维度信息
        //关联sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuInfoDS = AsyncDataStream.unorderedWait(productStatsWithSkuKeyDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                }, 60L, TimeUnit.SECONDS);

        //6.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuInfoDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print("productStatsWithTmDS>>>>>>>");


        //TODO 7.写入ClickHouse
        productStatsWithTmDS.addSink(ClickhouseUtil.getJdbcSink("insert into product_stats_210225 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute();
    }
}
