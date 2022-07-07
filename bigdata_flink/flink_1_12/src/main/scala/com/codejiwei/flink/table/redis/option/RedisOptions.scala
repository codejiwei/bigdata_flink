package com.codejiwei.flink.table.redis.option

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}

import java.lang
import java.time.Duration

/**
 * Author: codejiwei
 * Date: 2022/7/7
 * Desc:  
 * */
object RedisOptions {

  val HOSTNAME: ConfigOption[String] = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue()
    .withDescription("Options host for connect redis.")
  val PORT: ConfigOption[Integer] = ConfigOptions.key("port")
    .intType()
    .noDefaultValue()
    .withDescription("Options port for connect redis.")
  val LOOKUP_CACHE_TTL: ConfigOption[Duration] = ConfigOptions.key("lookup.cache.ttl")
    .durationType()
    .defaultValue(Duration.ofSeconds(0))
    .withDescription("Options for cache time to leave.")
  val LOOKUP_CACHE_MAX_ROWS: ConfigOption[lang.Long] = ConfigOptions.key("lookup.cache.max-rows")
    .longType()
    .defaultValue(-1L)
    .withDescription("Option for cache max number rows of lookup cache.")
}
