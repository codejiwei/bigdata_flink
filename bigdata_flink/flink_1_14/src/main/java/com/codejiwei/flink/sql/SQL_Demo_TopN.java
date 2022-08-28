package com.codejiwei.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/29
 * Desc:
 **/
public class SQL_Demo_TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE source_table (\n" +
                "    name BIGINT NOT NULL,\n" +
                "    search_cnt BIGINT NOT NULL,\n" +
                "    key BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen', \n" +
                "  'rows-per-second' = '5' \n" +
                ")");
        tEnv.executeSql("CREATE TABLE sink_table (\n" +
                "    key BIGINT,\n" +
                "    name BIGINT,\n" +
                "    search_cnt BIGINT,\n" +
                "    `timestamp` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tEnv.executeSql("INSERT INTO sink_table\n" +
                "SELECT key, name, search_cnt, row_time as `timestamp`\n" +
                "FROM (\n" +
                "   SELECT key, name, search_cnt, row_time, \n" +
                "     -- 根据热搜关键词 key 作为 partition key，然后按照 search_cnt 倒排取前 100 名\n" +
                "     ROW_NUMBER() OVER (PARTITION BY key\n" +
                "       ORDER BY search_cnt desc) AS rownum\n" +
                "   FROM source_table)\n" +
                "WHERE rownum <= 100");

    }
}
