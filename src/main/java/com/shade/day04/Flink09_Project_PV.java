package com.shade.day04;

import com.shade.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/4 19:18
 * @description:
 */
public class Flink09_Project_PV {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textFile = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> map = textFile.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3], Long.parseLong(split[4]));
            }
        });

        map.flatMap(new FlatMapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(UserBehavior value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    out.collect(Tuple2.of("pv", 1));
                }
            }
        }).keyBy(0).sum(1).print();


        //执行
        env.execute();
    }
}
