package com.shade.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: shade
 * @date: 2022/7/5 20:04
 * @description:
 */
public class Flink08_TimeWindow_Tumbling_WindowFun_AggFun {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(new Tuple2(s, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = flatMap.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> aggregate = window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            {
                System.out.println("创建对象");
            }
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("加");
                return accumulator += value.f1;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("输出");
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });
        aggregate.print();

        //执行
        env.execute();
    }
}
