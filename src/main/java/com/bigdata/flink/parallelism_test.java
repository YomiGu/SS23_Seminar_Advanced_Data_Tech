package com.bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class parallelism_test {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("taskmanager.numberOfTaskSlots",2);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 1. Create a data source
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.229.101", 9999).setParallelism(1);
        DataStream<Tuple2<String, Integer>> resultStream = stringDataStreamSource.flatMap(new FlatMapFunction<String,Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (int i = 0; i < s1.length; i++) {
                            collector.collect(new Tuple2<>(s1[i],1));
                        }
                    }
                }).setParallelism(1).slotSharingGroup("green")
                .keyBy(0)
                .sum(1).setParallelism(1).slotSharingGroup("red");

        resultStream.print().setParallelism(1);
        env.execute();
    }
}



