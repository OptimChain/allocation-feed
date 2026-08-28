package com.helsinki.marketdata.options

import com.helsinki.marketdata.config.AppConfig
import com.helsinki.marketdata.options.OptionsEncoders.given
import io.circe.Json
import io.circe.syntax.*
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

class OptionsRedisWriter(config: AppConfig):
  private val ChainHashKey   = "options-chain"
  private val HistoryKey     = "options-chain:history"

  private val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(4)
  poolConfig.setMaxIdle(2)

  private val pool = new JedisPool(
    poolConfig,
    config.redisHost,
    config.redisPort,
    5000,
    if config.redisPassword.nonEmpty then config.redisPassword else null
  )

  /** Write option chain snapshots for an underlying symbol to Redis. */
  def writeChain(underlying: String, snapshots: Seq[OptionSnapshot]): Unit =
    val jedis = pool.getResource
    try
      val pipe = jedis.pipelined()

      // Write each contract snapshot under options-chain:{UNDERLYING}
      val chainKey = s"$ChainHashKey:$underlying"
      for s <- snapshots do
        pipe.hset(chainKey, s.symbol, s.asJson.noSpaces)

      // Write chain summary metadata
      val meta = Json.obj(
        "underlying"   -> Json.fromString(underlying),
        "updated_at"   -> Json.fromString(java.time.Instant.now.toString),
        "num_contracts" -> Json.fromInt(snapshots.size),
        "contracts"    -> Json.fromValues(snapshots.map(s => Json.fromString(s.symbol))),
        "feed"         -> Json.fromString(config.optionsMarketDataFeed),
        "data_mode"    -> Json.fromString(config.optionsDataModeLabel)
      )
      pipe.hset(chainKey, "_meta", meta.noSpaces)
      pipe.expire(chainKey, 86400) // 24 hours

      // Also track which underlyings have chains in the top-level hash
      pipe.hset(ChainHashKey, underlying, meta.noSpaces)

      // Push chain snapshot into history
      val historyEntry = Json.obj(
        "timestamp"  -> Json.fromString(java.time.Instant.now.toString),
        "underlying" -> Json.fromString(underlying),
        "num_contracts" -> Json.fromInt(snapshots.size),
        "snapshots"  -> Json.fromValues(snapshots.map(_.asJson))
      )
      pipe.lpush(s"$HistoryKey:$underlying", historyEntry.noSpaces)
      pipe.ltrim(s"$HistoryKey:$underlying", 0, config.historyMaxSize - 1)
      pipe.expire(s"$HistoryKey:$underlying", 86400) // 24 hours

      pipe.sync()
    finally
      jedis.close()

  def ping(): Boolean =
    val jedis = pool.getResource
    try
      jedis.ping() == "PONG"
    catch
      case _: Exception => false
    finally
      jedis.close()

  def close(): Unit = pool.close()
