package com.shade.day04;

import com.shade.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/4 19:27
 * @description:
 */
public class Flink10_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> textFile = env.readTextFile("input/UserBehavior.csv");

        textFile.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            private Integer va = 0;

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                //传化为javabean
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3], Long.parseLong(split[4]));

                if ("pv".equals(userBehavior.getBehavior())) {
                    va += 1;
                }
                out.collect(Tuple2.of("pv", va));
            }
        }).setParallelism(1).print();

        //执行
        env.execute();
    }
}
