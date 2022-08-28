package com.codejiwei.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Author: codejiwei
 * Date: 2022/8/9
 * Desc:
 **/
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //添加Source
        DataStreamSource<String> streamSource = env.readTextFile("bigdata_flink/flink_1_12/src/main/resources/line.txt");
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        //数据处理
        streamSource.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute("WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word: line.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
