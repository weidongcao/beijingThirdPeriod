package com.nebuinfo.isec.center.stream.app.content.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

/**
 * Created by CaoWeiDong on 2018-04-25.
 */
public class SparkStreamingKafkaDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("demo").setMaster("local[4]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(8));
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

        //要监听的Topic
        Collection<String> topics = Arrays.asList("topicA", "topicB");

        //指定偏移量
        OffsetRange[] offsetRanges = {
                OffsetRange.create("test", 0, 0, 3),
                OffsetRange.create("test", 1, 0, 3)
        };

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream( streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams) );
        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                streamingContext.sparkContext(),
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );
        stream.foreachRDD(
                (VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd1 -> {
                    final OffsetRange[] offsetRanges1 = ((HasOffsetRanges) (rdd1.rdd())).offsetRanges();
                    rdd1.foreachPartition(
                            (VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecordIterator -> {
                                OffsetRange o = offsetRanges1[TaskContext.get().partitionId()];
                                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                            }
                    );
                }
        );


        // stream.mapToPair((PairFunction<ConsumerRecord<String, String>, String, String>) record -> new Tuple2<>(record.key(), record.value()));
    }
}
