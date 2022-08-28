package com.codejiwei.flink.sql;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/28
 * Desc:
 **/
public class SQL_Demo_OrderBy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000);
        //flink1.12设置状态后端的方式
//        env.setStateBackend(new FsStateBackend("hdfs:///"));
        //flink1.14设置状态后端的方式
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:///");
        //创建Table环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //source
        tEnv.executeSql("CREATE TABLE source_table (\n" +
                "    user_id BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.user_id.min' = '1',\n" +
                "  'fields.user_id.max' = '10'\n" +
                ")");

//        TableResult tableResult = tEnv.sqlQuery("select * from source_table").execute();
//
//        tableResult.print();

        //sink
        tEnv.executeSql("CREATE TABLE sink_table (\n" +
                "    user_id BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");


        tEnv.executeSql("INSERT INTO sink_table\n" +
                "SELECT user_id\n" +
                "FROM source_table\n" +
                "Order By row_time, user_id desc limit 3");




    }
}
