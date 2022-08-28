package com.codejiwei.flink.stream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Random;

/**
 * Author: codejiwei
 * Date: 2022/8/12
 * Desc:
 **/
public class TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Row> source = env.addSource(new MySource());
        source.map(new MapFunction<Row, Tuple2<Object, Object>>() {
            @Override
            public Tuple2<Object, Object> map(Row value) throws Exception {
                Object field = value.getField(1);
                Object field1 = value.getField(2);
                return Tuple2.of(field, field1);
            }
            //在keyby之前开窗 数据都会进入到一个窗口中去
        })
//                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))).max(1)
                .keyBy(value -> value.f0)
                .print();
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum(1)
//                .process(new Proce)

        env.execute();


    }
}
class MySource implements SourceFunction<Row> {

    private boolean existFlag = false;
    Random random = new Random();
    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (!existFlag) {
            ctx.collect(Row.of("random", random.nextInt(10), random.nextFloat()));
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        existFlag = true;
    }
}