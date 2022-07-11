package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink06_Keyd_AggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //转为javabean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //计算每个传感器的平均值
        map.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, Double>() {

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, Double>.Context ctx, Collector<Double> out) throws Exception {
                aggre.add(value);
                Tuple2<Integer, Integer> tuple2 = aggre.get();
                System.out.println(tuple2.f0);
                System.out.println(tuple2.f1);
                out.collect(tuple2.f0 * 1D / tuple2.f1);
            }

            private AggregatingState<WaterSensor, Tuple2<Integer, Integer>> aggre;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggre = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>("aggre", new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor value, Tuple2<Integer, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.getVc(), accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }, Types.TUPLE(Types.INT, Types.INT)));
            }


        }).print();


        env.execute();
    }
}
