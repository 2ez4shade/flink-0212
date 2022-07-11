package com.shade.day04;

import com.shade.bean.OrderEvent;
import com.shade.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author: shade
 * @date: 2022/7/4 20:11
 * @description:
 */
public class Flink15_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> orderDS = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> recDS = env.readTextFile("input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> ordmap = orderDS.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txmap = recDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");

                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));

            }
        });

        ConnectedStreams<OrderEvent, TxEvent> connect = ordmap.connect(txmap);

        ConnectedStreams<OrderEvent, TxEvent> keyBy = connect.keyBy("txId", "txId");

        keyBy.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            private HashMap<String,Long> ordermap = new HashMap<>();
            private HashSet<String> txset = new HashSet<>();

            @Override
            public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                if (txset.contains(value.getTxId())) {
                    out.collect("订单" + value.getOrderId() + "已经完成");
                    //移除txset缓存
                    txset.remove(value.getTxId());
                } else {
                    ordermap.put(value.getTxId(),value.getOrderId());
                }
            }

            @Override
            public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                if (ordermap.containsValue(value.getTxId())) {
                    out.collect("订单" + ordermap.get(value.getTxId())+ "已经完成");
                    ordermap.remove(value.getTxId());
                } else {
                    txset.add(value.getTxId());
                }
            }
        }).print();


        env.execute();
    }
}
