package com.codejiwei.flink.table.demo

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * Author: codejiwei
 * Date: 2022/7/8
 * Desc:  
 * */
object SQLSample extends App {
  //创建Stream执行环境
  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //设置并行度
  env.setParallelism(1)
  //创建Table环境
  private val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  //构造数据流
  val line = "hello world hello flink this is flink sql demo"
  private val words: Array[String] = line.split(" ")

  private val inputTable: Table = tEnv.fromDataStream(env.fromCollection(words))

  tEnv.createTemporaryView("input_table", inputTable)

  private val outTable: Table = tEnv.sqlQuery(
    """
      |select f0, count(1) from input_table group by f0""".stripMargin)

  tEnv.toRetractStream[Row](outTable).print()
//  tEnv.toAppendStream[Row](outTable).print()

  env.execute()
}
