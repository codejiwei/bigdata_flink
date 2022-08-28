package com.codejiwei.flink.stream.window;

import com.codejiwei.flink.demo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Author: codejiwei
 * Date: 2022/8/9
 * Desc:
 **/
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> sourceStream = env.socketTextStream("hadoop102", 8888);
        sourceStream.flatMap(new WordCount.Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute("Window WordCount");
    }
}
