package com.codejiwei.flink.stream.sink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

/**
 * Author: codejiwei
 * Date: 2022/8/10
 * Desc:
 **/
public class MySQLSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        //kafka source


    }
}
