package com.codejiwei.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/29
 * Desc:
 **/
public class SQL_Demo_WindowTopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE source_table (\n" +
                "    name STRING NOT NULL,\n" +
                "    search_cnt BIGINT NOT NULL,\n" +
                "    key BIGINT NOT NULL,\n" +
                "    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                "    WATERMARK FOR row_time AS row_time\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen', \n" +
                "  'fields.name.length' = '5', \n" +
                "  'fields.search_cnt.min' = '1', \n" +
                "  'fields.search_cnt.max' = '100', \n" +
                "  'fields.key.min' = '1', \n" +
                "  'fields.key.max' = '3', \n" +
                "  'rows-per-second' = '5' \n" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_table (\n" +
                "    key BIGINT,\n" +
                "    name STRING,\n" +
                "    search_cnt BIGINT,\n" +
                "    window_start TIMESTAMP(3),\n" +
                "    window_end TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tEnv.executeSql("INSERT INTO sink_table\n" +
                "SELECT key, name, search_cnt, window_start, window_end\n" +
                "FROM (\n" +
                "   SELECT key, name, search_cnt, window_start, window_end, \n" +
                "     ROW_NUMBER() OVER (PARTITION BY window_start, window_end, key\n" +
                "       ORDER BY search_cnt desc) AS rownum\n" +
                "   FROM (\n" +
                "      SELECT window_start, window_end, key, name, max(search_cnt) as search_cnt\n" +
                "      -- window tvf 写法\n" +
                "      FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(row_time), INTERVAL '10' SECONDS))\n" +
                "      GROUP BY window_start, window_end, key, name\n" +
                "   )\n" +
                ")\n" +
                "WHERE rownum <= 100");


    }
}
