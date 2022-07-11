package com.shade.day09;

import com.shade.part01.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: shade
 * @date: 2022/7/11 11:43
 * @description:
 */
public class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 读取数据源
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 转化为样例类
        SingleOutputStreamOperator<WaterSensor> javaBeanDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 添加水位线
        SingleOutputStreamOperator<WaterSensor> withWMDS = javaBeanDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 创建匹配对象
        Pattern<WaterSensor,WaterSensor>.begin("start").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return false;
            }
        })

        //TODO 获取匹配DStream

        //TODO select

        //TODO 输出

        env.execute();
    }
}
