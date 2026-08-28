package com.helsinki.marketdata.config

case class AppConfig(
  alpacaApiKey: String,
  alpacaSecretKey: String,
  redisHost: String,
  redisPort: Int,
  redisPassword: String,
  pollIntervalMs: Long,
  symbols: Seq[String],
  cryptoSymbols: Seq[String],
  historyMaxSize: Int,
  optionsSymbols: Seq[String],
  optionsPollIntervalMs: Long,
  /** false = indicative feed + 15m delay (free). true = OPRA realtime (Algo Trader Plus). */
  optionsRealtime: Boolean,
  optionsDelayMinutes: Int,
  // Options WebSocket stream config
  optionsStreamEnabled: Boolean,
  optionsStreamTicker: String,
  optionsStreamExpiration: String,
  optionsStreamStrikes: Seq[Double],
  optionsStreamTypes: Seq[String],
  optionsStreamFeed: String,
  // Dedicated Redis for options stream
  optionsStreamRedisHost: String,
  optionsStreamRedisPort: Int,
  optionsStreamRedisPassword: String
):
  /** Alpaca options REST feed: indicative (delayed, free) or opra (realtime, paid). */
  def optionsMarketDataFeed: String =
    if optionsRealtime then "opra" else "indicative"

  def optionsDataModeLabel: String =
    if optionsRealtime then
      "REALTIME (OPRA — requires Algo Trader Plus)"
    else
      s"DELAYED (${optionsDelayMinutes}m indicative — no market-data subscription)"

object AppConfig:
  def fromEnv(): AppConfig =
    val redisHostRaw = sys.env.getOrElse(
      "REDIS_HOST",
      "redis-17054.c99.us-east-1-4.ec2.cloud.redislabs.com:17054"
    )
    val (host, port) = redisHostRaw.split(":") match
      case Array(h, p) => (h, p.toIntOption.getOrElse(17054))
      case Array(h)    => (h, 17054)
      case _           => (redisHostRaw, 17054)

    val streamRedisRaw = sys.env.getOrElse(
      "OPTIONS_STREAM_REDIS_HOST",
      "redis-14697.c52.us-east-1-4.ec2.cloud.redislabs.com:14697"
    )
    val (streamHost, streamPort) = streamRedisRaw.split(":") match
      case Array(h, p) => (h, p.toIntOption.getOrElse(14697))
      case Array(h)    => (h, 14697)
      case _           => (streamRedisRaw, 14697)

    val optionsRealtime = sys.env.getOrElse("OPTIONS_REALTIME", "false").toBoolean
    val streamFeedRaw = sys.env.getOrElse("OPTIONS_STREAM_FEED", "indicative")
    // Delayed mode always uses indicative — OPRA stream requires Algo Trader Plus.
    val optionsStreamFeed = if optionsRealtime then streamFeedRaw else "indicative"

    AppConfig(
      alpacaApiKey = sys.env.getOrElse("ALPACA_API_KEY", ""),
      alpacaSecretKey = sys.env.getOrElse("ALPACA_SECRET_KEY", ""),
      redisHost = host,
      redisPort = port,
      redisPassword = sys.env.getOrElse("REDIS_PASSWORD", ""),
      pollIntervalMs = sys.env.getOrElse("POLL_INTERVAL_MS", "3000").toLong,
      symbols = sys.env.getOrElse("SYMBOLS", "BTC").split(",").map(_.trim).toSeq,
      cryptoSymbols = sys.env.getOrElse("CRYPTO_SYMBOLS", "BTC/USD").split(",").map(_.trim).toSeq,
      historyMaxSize = sys.env.getOrElse("HISTORY_MAX_SIZE", "10000").toInt,
      optionsSymbols = sys.env.getOrElse("OPTIONS_SYMBOLS", "NBIS,AVGO,SPY,IWN,MU").split(",").map(_.trim).toSeq,
      optionsPollIntervalMs = sys.env.getOrElse("OPTIONS_POLL_INTERVAL_MS", "30000").toLong,
      optionsRealtime = optionsRealtime,
      optionsDelayMinutes = sys.env.getOrElse("OPTIONS_DELAY_MINUTES", "15").toInt,
      optionsStreamEnabled = sys.env.getOrElse("OPTIONS_STREAM_ENABLED", "false").toBoolean,
      optionsStreamTicker = sys.env.getOrElse("OPTIONS_STREAM_TICKER", ""),
      optionsStreamExpiration = sys.env.getOrElse("OPTIONS_STREAM_EXPIRATION", ""),
      optionsStreamStrikes = sys.env.getOrElse("OPTIONS_STREAM_STRIKES", "").split(",").map(_.trim).filter(_.nonEmpty).map(_.toDouble).toSeq,
      optionsStreamTypes = sys.env.getOrElse("OPTIONS_STREAM_TYPES", "P").split(",").map(_.trim).filter(_.nonEmpty).toSeq,
      optionsStreamFeed = optionsStreamFeed,
      optionsStreamRedisHost = streamHost,
      optionsStreamRedisPort = streamPort,
      optionsStreamRedisPassword = sys.env.getOrElse("OPTIONS_STREAM_REDIS_PASSWORD", "")
    )
