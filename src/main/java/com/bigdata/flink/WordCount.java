package com.bigdata.flink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<Tuple2<String, Integer>> dataStream = env
//                .socketTextStream("192.168.229.101", 8888)
//                .flatMap(new Splitter())
//                .keyBy(value -> value.f0)
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1);

        // 1. 创建数据源
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.229.101", 8888);
        stringDataStreamSource.print("source");
        // 2. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new Splitter());
        tuple2SingleOutputStreamOperator.print("flatmap");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);
        // 3. 写出数据
        sum.print("sum");
        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}