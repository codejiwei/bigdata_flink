package com.codejiwei.flink.sql;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Author: codejiwei
 * Date: 2022/8/12
 * Desc:
 **/
public class SqlDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sqlDemo");

        DataStreamSource<String> sourceDs = env.addSource(new FlinkKafkaConsumer<String>("sqlDemo", new SimpleStringSchema(), properties));

        Table sourceTable = tEnv.fromDataStream(sourceDs);

        tEnv.createTemporaryView("inTable", sourceTable);

        Table resTable = tEnv.sqlQuery("select * from inTable");
        DataStream<Row> resDs = tEnv.toAppendStream(resTable, Row.class);

        resDs.print();

        env.execute();
    }
}
