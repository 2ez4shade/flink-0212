package com.shade.day04;

import com.alibaba.fastjson.JSONObject;
import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author: shade
 * @date: 2022/7/4 12:20
 * @description:
 */
public class Flink03_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> DS = env.socketTextStream("hadoop102", 9999);

//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "hadoop102:9092");
//
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("shadesensor", new SimpleStringSchema(), properties);

        SingleOutputStreamOperator<String> map = DS.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                String s = JSONObject.toJSONString(waterSensor);
                return s;
            }
        });
        map.print();
        map.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","shadesensor",new SimpleStringSchema()));

        env.execute();
    }
}
