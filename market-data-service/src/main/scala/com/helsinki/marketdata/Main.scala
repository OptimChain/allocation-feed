package com.helsinki.marketdata

import com.helsinki.marketdata.config.AppConfig
import com.helsinki.marketdata.poller.QuotePoller
import com.helsinki.marketdata.options.OptionsPoller
import com.helsinki.marketdata.stream.OptionsStreamPoller

@main def run(): Unit =
  println("[market-data] Initializing Alpaca -> Redis market data service")
  val config = AppConfig.fromEnv()

  if config.alpacaApiKey.isEmpty then
    System.err.println("[market-data] FATAL: ALPACA_API_KEY must be set")
    sys.exit(1)

  val quotePoller = QuotePoller(config)

  // Start options poller on a separate thread if options symbols are configured
  val optionsPoller = if config.optionsSymbols.nonEmpty then
    val op = OptionsPoller(config)
    val thread = new Thread(() => op.start(), "options-poller")
    thread.setDaemon(true)
    thread.start()
    println(s"[market-data] Options poller started for: ${config.optionsSymbols.mkString(", ")}")
    println(s"[market-data] Options data mode: ${config.optionsDataModeLabel}")
    Some(op)
  else
    None

  // Start options WebSocket stream if configured
  val optionsStream = if config.optionsStreamEnabled && config.optionsStreamTicker.nonEmpty then
    val sp = OptionsStreamPoller(config)
    val thread = new Thread(() => sp.start(), "options-stream")
    thread.setDaemon(true)
    thread.start()
    println(s"[market-data] Options stream started for: ${config.optionsStreamTicker} ${config.optionsStreamExpiration} strikes=${config.optionsStreamStrikes.mkString(",")} types=${config.optionsStreamTypes.mkString(",")}")
    Some(sp)
  else
    None

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    println("\n[market-data] Shutting down...")
    quotePoller.stop()
    optionsPoller.foreach(_.stop())
    optionsStream.foreach(_.stop())
  }))

  quotePoller.start()
