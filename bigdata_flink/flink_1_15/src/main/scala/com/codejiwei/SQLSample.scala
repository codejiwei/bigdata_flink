package com.codejiwei

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Author: jiwei01
 * Date: 2022/6/20 20:24
 * Package: com.codejiwei
 * Description:
 */
object SQLSample extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val tEnv = StreamTableEnvironment.create(env)

  val words = List("Hello World", "Hello Scala")

  val inputTable: Table = tEnv.fromDataStream(
    env.fromCollection(words).flatMap(_.toList))
  tEnv.createTemporaryView("input_table", inputTable)
  val outputTable: Table = tEnv.sqlQuery(
    """
      |SELECT f0, COUNT(*) FROM input_table
      |GROUP BY f0
      |""".stripMargin)

  tEnv.toChangelogStream(outputTable).print()

  env.execute()
}
