package com.helsinki.marketdata.options

import com.helsinki.marketdata.config.AppConfig
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

class OptionsPoller(config: AppConfig):
  private val client = OptionsClient(config)
  private val redis  = OptionsRedisWriter(config)
  @volatile private var running = true

  // How many top contracts (by volume) to fetch minute bars for
  private val TopContractsForBars = 20

  def start(): Unit =
    println(s"[options] Starting options poller")
    println(s"  underlyings: ${config.optionsSymbols.mkString(", ")}")
    println(s"  interval:    ${config.optionsPollIntervalMs}ms")
    println(s"  data mode:   ${config.optionsDataModeLabel}")
    println(s"  feed:        ${config.optionsMarketDataFeed}")
    println(s"  bars limit:  top $TopContractsForBars contracts per underlying")

    // Verify Redis
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
    // 1. Fetch option chain snapshots (near-term expirations)
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

      // Write chain to Redis
      try
        redis.writeChain(underlying, chain)
        println(s"[options] $underlying: chain written to Redis")
      catch
        case e: Exception =>
          println(s"[options] $underlying: Redis chain write failed: ${e.getMessage}")

      // 2. Pick top contracts by trade volume for minute bars
      val topContracts = chain
        .filter(_.latestTrade.isDefined)
        .sortBy(s => -s.latestTrade.map(_.size).getOrElse(0))
        .take(TopContractsForBars)
        .map(_.symbol)

      if topContracts.nonEmpty then
        val bars = client.fetchOptionBars(
          contractSymbols = topContracts,
          timeframe = "1Min",
          limit = 390  // Full trading day of minute bars
        )

        if bars.nonEmpty then
          val totalBars = bars.values.map(_.size).sum
          println(s"[options] $underlying: $totalBars minute bars across ${bars.size} contracts")

          try
            redis.writeBars(underlying, bars)
            println(s"[options] $underlying: bars written to Redis")
          catch
            case e: Exception =>
              println(s"[options] $underlying: Redis bars write failed: ${e.getMessage}")
        else
          println(s"[options] $underlying: no minute bars returned")
      else
        println(s"[options] $underlying: no traded contracts for bar fetching")
    else
      println(s"[options] $underlying: empty chain (market may be closed)")

  def stop(): Unit =
    running = false
    client.close()
    redis.close()
    println("[options] Stopped")
