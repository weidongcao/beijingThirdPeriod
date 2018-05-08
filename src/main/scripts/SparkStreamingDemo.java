package com.nebuinfo.isec.center.stream.app.content.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by CaoWeiDong on 2018-04-23.
 */
public class SparkStreamingDemo {

    @Test
    public void demo() throws InterruptedException {

        // String zkQuorm = "zookeeper.connect=cm01.spark.com:2181,cm02.spark.com:2181,cm03.spark.com:2181";
        // String zkQuorm = "zookeeper.connect=cm01.spark.com:9092,cm02.spark.com:9092,cm03.spark.com:9092";
        String kafkaListeners = "cm02.spark.com:9092";
        SparkConf conf = new SparkConf()
                .setAppName(SparkStreamingDemo.class.getSimpleName())
                .setMaster("local[4]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(8));
        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", "cm02.spark.com:9092");
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "jis");
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaPairDStream<String, String> pairDS = stream.mapToPair(
                (PairFunction<ConsumerRecord<String, String>, String, String>) record -> new Tuple2<>(record.key(), record.value())
        );
        pairDS.print();

        jssc.start();
        jssc.awaitTermination();
        // jssc.awaitTerminationOrTimeout(10);
    }
}
