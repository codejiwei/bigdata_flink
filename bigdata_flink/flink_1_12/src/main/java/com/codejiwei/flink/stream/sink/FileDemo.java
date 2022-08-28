package com.codejiwei.flink.stream.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/10
 * Desc:
 **/
public class FileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env.fromElements("hello", "flink", "hello", "java");
        sourceStream.map(v -> Tuple2.of(v, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(v -> v.f0)
                .sum(1)
                .writeAsCsv("bigdata_flink/flink_1_12/src/main/outFile/out.csv");
//                .writeAsText("bigdata_flink/flink_1_12/src/main/outFile/text.txt");
        env.execute();


    }
}
