package com.shade.day04;

import com.shade.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: shade
 * @date: 2022/7/4 19:49
 * @description:
 */
public class Flink12_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MarketingUserBehavior> source = env.addSource(new MySource());

        source.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "-" + value.getBehavior(), 1);
            }
        }).keyBy(0).sum(1).print();

        //执行
        env.execute();


    }

    public static class MySource extends RichSourceFunction<MarketingUserBehavior> {

        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                ctx.collect(
                        new MarketingUserBehavior(
                        (long) random.nextInt(100000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                ));

                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
