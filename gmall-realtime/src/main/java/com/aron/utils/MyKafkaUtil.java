package com.aron.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


public class MyKafkaUtil {

    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static Properties properties = new Properties();

    //默认主题
    private static String defaultTopic = "dwd_fact";

    /**
     * 适用不固定的topic，并使用泛型方法可以传任意类型的数据
     *
     * @param kafkaSerializationSchema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        return new FlinkKafkaProducer<T>(defaultTopic, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 根据输入的参数，往对应的topic写数据
     *
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    /**
     * 获取KafkaSource的方法
     *
     * @param topic   主题
     * @param groupId 消费者组
     * @return
     */
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {
        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        //获取kafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "', " +
                " 'properties.bootstrap.servers' = '" + kafkaServer + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset') ";
    }
}
