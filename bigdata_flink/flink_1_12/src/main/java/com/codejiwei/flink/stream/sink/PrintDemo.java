package com.codejiwei.flink.stream.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.awt.*;
import java.util.Arrays;


/**
 * Author: codejiwei
 * Date: 2022/8/10
 * Desc:
 **/
public class PrintDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String[] words = "hello flink hello scala".split(" ");

        DataStreamSource<String> sourceStream = env.fromCollection(Arrays.asList(words.clone()));

        sourceStream.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(v -> v.f0)
                .sum(1)
                //在标准错误上打印输出
                .printToErr();
        //在标准输出打印
//                .print();
        env.execute();

    }
}
