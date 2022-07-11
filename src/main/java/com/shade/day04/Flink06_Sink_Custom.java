package com.shade.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author: shade
 * @date: 2022/7/4 18:34
 * @description:
 */
public class Flink06_Sink_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

        ds.addSink(new RichSinkFunction<String>() {
            Connection connection = null;
            PreparedStatement preparedStatement = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
                //获取预执行语句
                preparedStatement = connection.prepareStatement("insert into flinktest values (?,?,?)");
            }

            @Override
            public void close() throws Exception {
                preparedStatement.close();
                connection.close();
            }

            @Override
            public void invoke(String value, Context context) throws Exception {


                String[] split = value.split(",");
                preparedStatement.setString(1, split[0]);
                preparedStatement.setString(2, split[1]);
                preparedStatement.setLong(3, Long.parseLong(split[2]));
                //执行
                preparedStatement.execute();

                //关闭资源


            }
        });

        env.execute();
    }
}
