package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.swing.*;


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink02_Keyd_Value {
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

        map.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            private ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                Integer lastvalue = state.value() == null ? value.getVc() : state.value();
                if (Math.abs(lastvalue - value.getVc()) > 10) {
                    out.collect(value.getId() + "警报！！警报！！");
                }
                state.update(value.getVc());
            }
        }).print();

        env.execute();
    }
}
