package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink09_Keyd_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);
        // 定义状态并广播
        MapStateDescriptor<String, String> state = new MapStateDescriptor<>("state", String.class, String.class);

        BroadcastStream<String> broadcast = controlStream.broadcast(state);

        BroadcastConnectedStream<String, String> connect = dataStream.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //读广播map
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(state);
                if ("1".equals(broadcastState.get("switch"))) {
                    out.collect("切换到1号配置....");
                } else if ("0".equals(broadcastState.get("switch"))) {
                    out.collect("切换到0号配置....");
                } else {
                    out.collect("切换到其他配置....");
                }

            }

            //往广播map里写
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(state);
                broadcastState.put("switch", value);
            }
        }).print();


        env.execute();
    }

}
