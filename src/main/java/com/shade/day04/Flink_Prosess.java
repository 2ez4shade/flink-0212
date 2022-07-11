package com.shade.day04;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author: shade
 * @date: 2022/7/4 9:20
 * @description:
 */
public class Flink_Prosess {
    public static void main(String[] args) throws Exception {
//        HashMap<String, Integer> map = new HashMap<>();
//
//        System.out.println(map.getOrDefault("xxx",0));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = socketDS.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, ProcessFunction<String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyBy = map.keyBy("id");


        SingleOutputStreamOperator<WaterSensor> process = keyBy.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            private HashMap<String, Integer> map2 = new HashMap();

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                Integer vcValue = map2.getOrDefault(value.getId(), 0);
                vcValue += value.getVc();
                map2.put(value.getId(), vcValue);
                out.collect(new WaterSensor(value.getId(), value.getTs(), vcValue));
            }
        });

        map.print("map");
        process.print("sum");


        env.execute();
    }
}
