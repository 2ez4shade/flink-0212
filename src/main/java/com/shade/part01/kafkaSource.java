package com.shade.part01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.util.Properties;

/**
 * @author: shade
 * @date: 2022/7/2 10:46
 * @description:
 */
public class kafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop102:9092")
//                .setGroupId("shade")
//                .setTopics(Arrays.asList("sensor"))
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
//
//
//        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

//        kafkaDS.print();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "shade");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource kfDS = env.addSource(new FlinkKafkaConsumer("sensor", new SimpleStringSchema(), properties));


//        kfDS.map(new MapFunction<String, WaterSensor>() {
//            @Override
//            public WaterSensor map(String value) throws Exception {
//                String[] split = value.split(",");
//                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//            }
//
//        }).print();
        kfDS.map(new RichMapFunction<String, WaterSensor>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open.....");
            }

            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }

            @Override
            public void close() throws Exception {
                System.out.println("close......");
            }
        }).print();



        kfDS.print();

        env.execute();
    }
}
