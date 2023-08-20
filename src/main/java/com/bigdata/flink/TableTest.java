package com.bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> socketStream = env.socketTextStream("192.168.229.101", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> datas = socketStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                String[] s1 = s.split(" ");
                collector.collect(new Tuple3<>(s1[0], s1[1], Long.valueOf(s1[2])));
            }
        });

        Table table = streamTableEnv.fromDataStream(datas, $("id"),$("name"),$("timestamp"));


        Table select = table.groupBy($("id")).select($("id"), $("name").count().as("cnt"));

        select.execute().print();

    }
}
