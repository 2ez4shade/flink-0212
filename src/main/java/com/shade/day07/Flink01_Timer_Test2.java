package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
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
public class Flink01_Timer_Test2 {
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
            Long timer = Long.MIN_VALUE;
            Integer vc = Integer.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                // 如果增长的话就注册定时器（timer为Long的最小值说明已经注册过了）
                if (value.getVc() > vc) {
                    if (timer == Long.MIN_VALUE) {
                        timer = ctx.timerService().currentProcessingTime()+5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    if (timer != Long.MIN_VALUE) {

                        ctx.timerService().deleteProcessingTimeTimer(timer);
                        timer = Long.MIN_VALUE;
                    }
                }
                vc = value.getVc();
                out.collect("当前水位线"+vc);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                ctx.output(new OutputTag<String>("output") {
                }, "警告水位线已经到达" + vc + ",请注意!!!!");

            }
        });
        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output") {
        }).print("分流");


        env.execute();
    }
}