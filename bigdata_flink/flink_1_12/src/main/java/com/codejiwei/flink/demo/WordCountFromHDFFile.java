package com.codejiwei.flink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Author: codejiwei
 * Date: 2022/8/10
 * Desc:
 **/
public class WordCountFromHDFFile {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        FileSystem fs = FileSystem.get(new Configuration());
//        env.readFile(fs.file)


    }
}
