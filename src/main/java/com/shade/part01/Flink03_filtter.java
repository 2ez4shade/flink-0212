package com.shade.part01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: shade
 * @date: 2022/7/2 19:58
 * @description:
 */
public class Flink03_filtter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                if (value % 2 == 0) {
                    return true;
                }
                return false;
            }
        }).print();

        env.execute();
    }
}
