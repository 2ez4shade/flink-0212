package com.shade.part01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/2 14:38
 * @description:
 */
public class Flink04_Keyby {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

//        ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] s = value.split(" ");
//                for (String s1 : s) {
//                    out.collect(Tuple2.of(s1, 1));
//                }
//            }
//        }).keyBy(0).sum(1).print();

//        KeyedStream<WaterSensor, String> n = ds.map(new MapFunction<String, WaterSensor>() {
//            @Override
//            public WaterSensor map(String value) throws Exception {
//                String[] split = value.split(" ");
//                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
//            }
//        }).keyBy(new KeySelector<WaterSensor, String>() {
//            @Override
//            public String getKey(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });
//        n.max("vc").print("max");
//        n.maxBy("vc", true).print("bymax");
        ds.shuffle().print();



        env.execute();
    }
}
