package com.codejiwei.flink.stream.souce;

import com.codejiwei.flink.demo.WordCount;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Author: codejiwei
 * Date: 2022/8/10
 * Desc:
 **/
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new MemoryStateBackend());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafkaDemo1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("test1", new SimpleStringSchema(), properties));
        streamSource.flatMap(new WordCount.Splitter())
                .keyBy(v -> v.f0)
                .sum(1)
                .print();
//        env.addSource(new FlinkKafkaConsumer<Row>("test1", n))
        env.execute();


    }
}
