package com.codejiwei.cache

import org.apache.flink.shaded.guava18.com.google.common.cache._
import org.apache.flink.types.Row

import java.util.concurrent.TimeUnit

/**
 * Author: jiwei01
 * Date: 2022/7/5 15:31
 * Package: 
 * Description: 封装GuavaCache实现范型
 */
object GuavaCacheTest {

  def main(args: Array[String]): Unit = {

    val cache = LookupCache[Seq[AnyRef], List[Row]](1, 1)
    val keys = Seq("Tom")
    val rows = List(Row.of("""{"age": 18, "gender":"men"}"""))

//    println(cache.getIfPresent(keys))
//    cache.put(keys, rows)
//    println(cache.getIfPresent(keys))


    val cache1 = LookupCache[String, Integer](1, 1)
    val key1 = "Jerry"
    val row1 = 25
    println(cache1.getIfPresent(key1))
    cache1.put(key1, row1)
    println(cache1.getIfPresent(key1))
  }
}

class LookupCache[K <: Object, V<: Object](ttl: Int, size: Int) {
  var cache: Cache[K, V] = _
  init()
  private def init(): Unit = {
    println("init....")
    this.cache = CacheBuilder.newBuilder()
      .maximumSize(size)
      .expireAfterWrite(ttl, TimeUnit.SECONDS)
      .build[K, V]()
  }
  def getIfPresent(keys: K): V = {
    cache.getIfPresent(keys)
  }

  def put(keys: K, value: V): Unit = {
    cache.put(keys, value)
  }
}

object LookupCache {
  def apply[K <: Object, V <: Object](ttl: Int, size: Int): LookupCache[K, V] = new LookupCache(ttl, size)
}