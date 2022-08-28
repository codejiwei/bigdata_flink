package com.codejiwei.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/28
 * Desc:
 **/
public class SQL_Demo_UpsertKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE pageviews_per_region_source (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT\n" +
//                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'pageviews_per_region_source',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE pageviews_per_region_sink (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'pageviews_per_region_sink',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json',\n" +
                "  'sink.buffer-flush.interval' = '10000', \n" +
                "  'sink.buffer-flush.max-rows' = '2' \n" +
                ")");


        tEnv.executeSql("insert into pageviews_per_region_sink select * from pageviews_per_region_source");


    }
}
