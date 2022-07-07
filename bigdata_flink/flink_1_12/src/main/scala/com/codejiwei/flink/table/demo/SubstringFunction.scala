package com.codejiwei.flink.table.demo

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * Author: codejiwei
 * Date: 2022/7/8
 * Desc:  
 * */
class SubstringFunction extends ScalarFunction{
  def eval(s: String, begin: Integer, end: Integer): String = {
    s.substring(begin, end)
  }

}

object SubstringDemoTest extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val tableEnv = StreamTableEnvironment.create(env)

  val words = List("Hello World", "Hello Scala")

  private val inputTable: Table = tableEnv.fromDataStream(env.fromCollection(words).flatMap(_.toList))

  tableEnv.createTemporaryView("inputTable", inputTable)

  private val outTable: Table = tableEnv.sqlQuery(
    """
      |select f0 from inputTable""".stripMargin)
  tableEnv.toAppendStream[Row](outTable).print()

  env.execute()

}
