package com.shade.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author: shade
 * @date: 2022/7/4 19:06
 * @description:
 */
public class Flink08_WordCount_RuntimeMode {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //读无解流会报错
       // env.setRuntimeMode(RuntimeExecutionMode.BATCH);


//        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketDS = env.readTextFile("input/word.txt");

        socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        }).keyBy(0).sum(1).print();

        //执行
        env.execute();
    }
}
