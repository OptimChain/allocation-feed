package com.helsinki.marketdata.options

import com.helsinki.marketdata.config.AppConfig
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class OptionsPoller(config: AppConfig):
  private val client = OptionsClient(config)
  private val redis  = OptionsRedisWriter(config)
  @volatile private var running = true

  def start(): Unit =
    println(s"[options] Starting options poller")
    println(s"  underlyings: ${config.optionsSymbols.mkString(", ")}")
    println(s"  interval:    ${config.optionsPollIntervalMs}ms")
    println(s"  data mode:   ${config.optionsDataModeLabel}")
    println(s"  feed:        ${config.optionsMarketDataFeed}")

    try
      if redis.ping() then
        println("[options] Redis connection OK")
      else
        println("[options] WARNING: Redis ping failed — will retry on writes")
    catch
      case e: Exception =>
        println(s"[options] WARNING: Redis ping error: ${e.getMessage}")

    while running do
      try
        for underlying <- config.optionsSymbols do
          pollUnderlying(underlying)

        Thread.sleep(config.optionsPollIntervalMs)
      catch
        case _: InterruptedException => running = false
        case e: Exception =>
          println(s"[options] ERROR: ${e.getMessage}")
          Thread.sleep(config.optionsPollIntervalMs)

  private def pollUnderlying(underlying: String): Unit =
    val today = LocalDate.now()
    val nearTermEnd = today.plusDays(45).format(DateTimeFormatter.ISO_LOCAL_DATE)

    val chain = client.fetchOptionChain(
      underlying = underlying,
      feed = config.optionsMarketDataFeed,
      expirationGte = Some(today.format(DateTimeFormatter.ISO_LOCAL_DATE)),
      expirationLte = Some(nearTermEnd),
      limit = 100
    )

    if chain.nonEmpty then
      println(s"[options] $underlying: ${chain.size} contracts in chain")

      try
        redis.writeChain(underlying, chain)
        println(s"[options] $underlying: chain written to Redis")
      catch
        case e: Exception =>
          println(s"[options] $underlying: Redis chain write failed: ${e.getMessage}")
    else
      println(s"[options] $underlying: empty chain (market may be closed)")

  def stop(): Unit =
    running = false
    client.close()
    redis.close()
    println("[options] Stopped")
