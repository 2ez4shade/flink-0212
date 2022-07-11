package com.shade.day04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: shade
 * @date: 2022/7/4 12:09
 * @description:
 */
public class Flink02_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> map = socketDS.map(r -> r).setParallelism(2);

        map.print("原始").setParallelism(2);
        map.keyBy(r -> r).print("keyby");;
        map.rebalance().print("rebalance");
        map.shuffle().print("shuffle");
        map.rescale().print("rescale");





        env.execute();
    }
}
