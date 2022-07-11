package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink04_Keyd_ListState {
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
        //针对每个传感器输出最高的3个水位值
        map.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //liststate
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
               listState= getRuntimeContext().getListState(new ListStateDescriptor<Integer>("ListState", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                listState.add(value.getVc());
                //创建list接收数据
                ArrayList<Integer> list = new ArrayList<>();
                for (Integer in : listState.get()) {
                    list.add(in);
                }
                //排序只要前3
                list.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });

                if (list.size() > 3) {
                    list.remove(3);
                }
                listState.update(list);
                out.collect(list.toString());
            }
        }).print();



        env.execute();
    }
}
