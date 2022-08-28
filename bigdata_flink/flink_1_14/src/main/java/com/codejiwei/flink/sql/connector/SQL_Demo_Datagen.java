package com.codejiwei.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/28
 * Desc:
 **/
public class SQL_Demo_Datagen {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    buyer        ROW<first_name STRING, last_name STRING>,\n" +
                "    order_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen', \n" +
                "  'rows-per-second' = '5', \n" +
                "  'fields.order_number.kind' = 'sequence', \n" +
                "  'fields.order_number.start' = '1', \n" +
                "  'fields.order_number.end' = '10', \n" +

                "  'fields.price.min' = '10', \n" +
                "  'fields.price.max' = '100', \n" +

                "  'fields.buyer.first_name.length' = '5', \n" +
                "  'fields.buyer.last_name.length' = '5' \n" +
                ")");

        TableResult tableResult = tEnv.sqlQuery("select * from Orders").execute();
        tableResult.print();

    }
}
