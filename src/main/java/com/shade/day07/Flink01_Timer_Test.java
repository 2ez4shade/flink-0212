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


/**
 * @author: shade
 * @date: 2022/7/8 9:46
 * @description:
 */
public class Flink01_Timer_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        SingleOutputStreamOperator<String> process = map.keyBy("id").process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {


            private ValueState<Long> timer;
            private ValueState<Integer> v;

            @Override
            public void open(Configuration parameters) throws Exception {
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                v = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("v", Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                if (timer.value()==null){
                    timer.update(Long.MIN_VALUE);
                }
                if (v.value()==null){
                    v.update(Integer.MIN_VALUE);
                }

                if (value.getVc() > v.value()) {
                    //注册
                    if (timer.value() == Long.MIN_VALUE) {
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    if (timer.value() != Long.MIN_VALUE) {
                        ctx.timerService().deleteProcessingTimeTimer(timer.value());
                        timer.update(Long.MIN_VALUE);
                    }
                }
                v .update( value.getVc());
                out.collect("主流的水位为"+value.getVc());
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                timer.update(Long.MIN_VALUE);
                ctx.output(new OutputTag<String>("output") {
                }, "警告>>>> 5s内水位连续上升");
            }
        });

        process.getSideOutput((new OutputTag<String>("output") {
        })).print("分流");
        process.print("主流");

        env.execute();
    }
}
