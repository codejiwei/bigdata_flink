package com.codejiwei.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/28
 * Desc:
 **/
public class SQL_Demo_Print {
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
                "  'fields.order_number.min' = '1', \n" +
                "  'fields.order_number.max' = '10', \n" +

                "  'fields.price.min' = '10', \n" +
                "  'fields.price.max' = '100', \n" +

                "  'fields.buyer.first_name.length' = '5', \n" +
                "  'fields.buyer.last_name.length' = '5' \n" +
                ")");

//        tEnv.executeSql("CREATE TABLE print_table WITH ('connector' = 'print')\n" +
//                "LIKE Orders (EXCLUDING ALL)");
        tEnv.executeSql("CREATE TEMPORARY TABLE print_table WITH (" +
                "'connector' = 'print',\n" +

                //输出的前缀标识符
                "'print-identifier' = 'test',\n" +

                //打印到标准错误，而不是标准输出
                "'standard-error' = 'false',\n" +

                //定义sink的并行度 默认和上游链式运算的算子一样
                "'sink.parallelism' = '1')\n" +

                "LIKE Orders (EXCLUDING ALL)");


        tEnv.executeSql("insert into print_table select * from Orders");


    }
}
