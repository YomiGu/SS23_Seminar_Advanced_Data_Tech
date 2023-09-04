package com.bigdata.flink;

import org.apache.commons.logging.Log;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkProcessFunctionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. 创建数据源
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.229.101", 9999);

        stringDataStreamSource.print("source");


        SingleOutputStreamOperator<Tuple2<String, String>> map = stringDataStreamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            //tuple中的第一个指的是ID 第二个便是时间戳
            public Tuple2<String, String> map(String s) throws Exception {
                String[] s1 = s.split(" ");
                return new Tuple2<>(s1[0], s1[1]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, String>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple2<String, String> element) {
                return Long.valueOf(element.f1);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Long>> process = map.keyBy(value -> value.f0)
                .process(new CountWithTimeoutFunction());

        process.print();

        env.execute();

    }

}
