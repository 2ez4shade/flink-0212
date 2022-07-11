package day06;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: shade
 * @date: 2022/7/6 18:45
 * @description:
 */
public class Flink06_EventTime_WaterMark_SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //设置周期值
        env.getConfig().setAutoWatermarkInterval(300);


        env.setParallelism(1);

        //转为对象流
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //设置水位线

        //乱序流
        SingleOutputStreamOperator<WaterSensor> operator1 = map.assignTimestampsAndWatermarks(
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyWatermarkGenerator(3L);
                    }
                }
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        WindowedStream<WaterSensor, Tuple, TimeWindow> window = operator1.
                keyBy("id").
                window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .allowedLateness(Time.milliseconds(3)).
                sideOutputLateData(new OutputTag<WaterSensor>("output"){});

        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple key, ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() + "," + context.window().getEnd() + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        });

        process.getSideOutput(new OutputTag<WaterSensor>("output") {
        }).print("测流");

        process.print("主流");

        env.execute();
    }


   public static class MyWatermarkGenerator implements WatermarkGenerator<WaterSensor> {
        /**
         * The maximum timestamp encountered so far.
         */
        private long maxTimestamp;

        /**
         * The maximum out-of-orderness that this watermark generator assumes.
         */
        private final long outOfOrdernessMillis;

        public MyWatermarkGenerator(Long time) {
            outOfOrdernessMillis = time;
            maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //System.out.println("更新maxtime");
            maxTimestamp = Math.max(eventTimestamp, maxTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
           // System.out.println("生成watermark。。。");
            output.emitWatermark(new Watermark(maxTimestamp-outOfOrdernessMillis-1));
        }
    }

}
