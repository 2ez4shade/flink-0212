package com.shade.part01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author: shade
 * @date: 2022/7/2 12:41
 * @description:
 */
public class UserSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFunction<WaterSensor>() {
            private Random random = new Random();
            private Boolean bool = true;

            @Override
            public void run(SourceContext<WaterSensor> ctx) throws Exception {
                while (true) {
                    WaterSensor sensor = new WaterSensor("sensor" + random.nextInt(1000), random.nextInt(2000), random.nextInt(10));
                    ctx.collect(sensor);
                }
            }

            @Override
            public void cancel() {
                bool = false;
            }
        }).print();


        env.execute();
    }
}
