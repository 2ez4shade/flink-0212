package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink08_Keyd_MapState2 {
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


        map.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            private MapState<Integer, WaterSensor> state;

            @Override
            public void open(Configuration parameters) throws Exception {

                state = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("state", Integer.class, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if (!state.contains(value.getVc())) {
                    out.collect(value);

                    state.put(value.getVc(), value);
                }

            }
        }).print();


        env.execute();
    }

}
