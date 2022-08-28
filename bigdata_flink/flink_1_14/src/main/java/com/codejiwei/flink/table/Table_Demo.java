package com.codejiwei.flink.table;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptionsUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: codejiwei
 * Date: 2022/8/29
 * Desc:
 **/
public class Table_Demo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //创建source表
        tEnv.createTemporaryTable("source_table",
                TableDescriptor.forConnector("datagen")
        .schema(Schema.newBuilder()
        .column("f0", DataTypes.STRING())
        .build())
        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 5L)
//                        .option(DataGenConnectorOptions.FIELD_MIN.withFallbackKeys("f0"), "1")
//                        .option(DataGenConnectorOptions.FIELD_MAX.withDeprecatedKeys("f0"), "10")
        .build());

        Table table = tEnv.sqlQuery("select * from source_table");
        table.execute().print();


    }
}
