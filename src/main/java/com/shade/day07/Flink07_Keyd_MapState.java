package com.shade.day07;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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
public class Flink07_Keyd_MapState {
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
        //添加水位
        SingleOutputStreamOperator<WaterSensor> operator = map.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        SingleOutputStreamOperator<Double> process = operator.keyBy("id").process(new MyKeyFunction(10000L));

        process.print();
        process.getSideOutput(new OutputTag<Double>("xxx") {
        }).print("测输出");

        env.execute();
    }

    public static class MyKeyFunction extends KeyedProcessFunction<Tuple, WaterSensor, Double> {
        //窗口大小
        private final Long windowsize;
        private MapState<Long, Tuple2<Integer, Integer>> mapState;

        public MyKeyFunction(Long windowsize) {
            this.windowsize = windowsize;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Tuple2<Integer, Integer>>("mapState", Types.LONG, Types.TUPLE(Types.INT, Types.INT)));
        }


        @Override
        public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, Double>.Context ctx, Collector<Double> out) throws Exception {

            long ts = value.getTs();
            long windowstart = ts*1000 / windowsize * windowsize;
            long windowend = windowstart + windowsize;

            //获取窗口中的最大时间戳来 定时-1ms

            if (!mapState.contains(windowstart)) {
                mapState.put(windowstart, Tuple2.of(0, 0));
                System.out.println("在" + windowstart + "时定了个时");
                ctx.timerService().registerEventTimeTimer(windowend - 1);
            }
            Tuple2<Integer, Integer> tuple2 = mapState.get(windowstart);

            mapState.put(windowstart,Tuple2.of(tuple2.f0+value.getVc(),tuple2.f1+1));

        }
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, Double>.OnTimerContext ctx, Collector<Double> out) throws Exception {
            System.out.println(timestamp+"------定时器触发");
            Tuple2<Integer, Integer> tuple2 = mapState.get(timestamp + 1 - windowsize);
            System.out.println(tuple2.f0+"值！");
            System.out.println(tuple2.f1+"个数！");
            System.out.println(tuple2.f0*1D/tuple2.f1);
       //     ctx.output(new OutputTag<Double>("xxx"){},tuple2.f0*1D/tuple2.f1);
            //模拟窗口的关闭
            mapState.remove(timestamp + 1 - windowsize);

        }

    }
}
