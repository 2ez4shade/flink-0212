package com.shade.part01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author shade
 * @date 2022/7/1 19:13
 * @description
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建ds
        DataStreamSource<String> stream1 = env.readTextFile("input/watersensors");

        //读取数组
        ArrayList<WaterSensor> sensors = new ArrayList<>();
        WaterSensor sensor1 = new WaterSensor("1", 1000, 20);
        WaterSensor sensor2 = new WaterSensor("2", 2000, 30);
        WaterSensor sensor3 = new WaterSensor("3", 3000, 90);
        sensors.add(sensor1);
        sensors.add(sensor2);
        sensors.add(sensor3);

        DataStreamSource<WaterSensor> stream2 = env.fromCollection(sensors);

        DataStreamSource<WaterSensor> stream3 = env.fromElements(new WaterSensor("1", 1000, 20), new WaterSensor("2", 2000, 30));


        stream1.print("1");
        stream2.print("2");
        stream3.print("3");


        env.execute();
    }
}
