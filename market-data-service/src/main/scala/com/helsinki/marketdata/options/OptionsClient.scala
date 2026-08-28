package com.helsinki.marketdata.options

import com.helsinki.marketdata.config.AppConfig
import io.circe.parser.*
import io.circe.*
import sttp.client3.*

class OptionsClient(config: AppConfig):
  private val backend = HttpClientSyncBackend()
  private val optionsBaseUrl = "https://data.alpaca.markets/v1beta1/options"

  /** Fetch the full options chain (snapshots) for an underlying symbol.
    * Returns contract snapshots with latest trade, latest quote, and greeks.
    */
  def fetchOptionChain(
    underlying: String,
    feed: String = "indicative",
    optionType: Option[String] = None,
    expirationGte: Option[String] = None,
    expirationLte: Option[String] = None,
    strikePriceGte: Option[Double] = None,
    strikePriceLte: Option[Double] = None,
    limit: Int = 100
  ): Seq[OptionSnapshot] =
    try
      var url = uri"$optionsBaseUrl/snapshots/$underlying?feed=$feed&limit=$limit"

      // Build query params manually since sttp uri interpolation handles it
      val params = Seq(
        optionType.map(t => s"type=$t"),
        expirationGte.map(d => s"expiration_date_gte=$d"),
        expirationLte.map(d => s"expiration_date_lte=$d"),
        strikePriceGte.map(p => s"strike_price_gte=$p"),
        strikePriceLte.map(p => s"strike_price_lte=$p")
      ).flatten

      val fullUrl = if params.nonEmpty then
        val baseStr = s"$optionsBaseUrl/snapshots/$underlying?feed=$feed&limit=$limit&${params.mkString("&")}"
        uri"$baseStr"
      else
        uri"$optionsBaseUrl/snapshots/$underlying?feed=$feed&limit=$limit"

      val response = basicRequest
        .get(fullUrl)
        .header("APCA-API-KEY-ID", config.alpacaApiKey)
        .header("APCA-API-SECRET-KEY", config.alpacaSecretKey)
        .send(backend)

      response.body match
        case Right(body) => parseOptionChain(body, underlying)
        case Left(err) =>
          println(s"[options] Chain error for $underlying: $err")
          Seq.empty
    catch
      case e: Exception =>
        println(s"[options] Chain exception for $underlying: ${e.getMessage}")
        Seq.empty

  /** Fetch latest snapshots for specific contract symbols. */
  def fetchOptionSnapshots(
    contractSymbols: Seq[String],
    feed: String = "indicative"
  ): Seq[OptionSnapshot] =
    if contractSymbols.isEmpty then return Seq.empty

    try
      val symbolsParam = contractSymbols.take(100).mkString(",")
      val response = basicRequest
        .get(uri"$optionsBaseUrl/snapshots?symbols=$symbolsParam&feed=$feed")
        .header("APCA-API-KEY-ID", config.alpacaApiKey)
        .header("APCA-API-SECRET-KEY", config.alpacaSecretKey)
        .send(backend)

      response.body match
        case Right(body) => parseSnapshots(body)
        case Left(err) =>
          println(s"[options] Snapshots error: $err")
          Seq.empty
    catch
      case e: Exception =>
        println(s"[options] Snapshots exception: ${e.getMessage}")
        Seq.empty

  // --- Parsers ---

  private def parseOptionChain(body: String, underlying: String): Seq[OptionSnapshot] =
    parse(body).toOption match
      case Some(json) =>
        val snapshots = json.hcursor.downField("snapshots")
        snapshots.keys.getOrElse(Nil).toSeq.flatMap { contractSymbol =>
          val c = snapshots.downField(contractSymbol)
          Some(parseOneSnapshot(c, contractSymbol))
        }
      case None =>
        println(s"[options] Failed to parse chain JSON for $underlying")
        Seq.empty

  private def parseSnapshots(body: String): Seq[OptionSnapshot] =
    parse(body).toOption match
      case Some(json) =>
        val snapshots = json.hcursor.downField("snapshots")
        snapshots.keys.getOrElse(Nil).toSeq.flatMap { contractSymbol =>
          val c = snapshots.downField(contractSymbol)
          Some(parseOneSnapshot(c, contractSymbol))
        }
      case None =>
        println(s"[options] Failed to parse snapshots JSON")
        Seq.empty

  private def parseOneSnapshot(c: ACursor, contractSymbol: String): OptionSnapshot =
    val trade = for
      price <- c.downField("latestTrade").get[Double]("p").toOption
      size  <- c.downField("latestTrade").get[Int]("s").toOption
      ts    <- c.downField("latestTrade").get[String]("t").toOption
    yield OptionTrade(price, size, ts)

    val quote = for
      bp <- c.downField("latestQuote").get[Double]("bp").toOption
      ap <- c.downField("latestQuote").get[Double]("ap").toOption
      bs <- c.downField("latestQuote").get[Int]("bs").toOption
      as_ <- c.downField("latestQuote").get[Int]("as").toOption
      ts <- c.downField("latestQuote").get[String]("t").toOption
    yield OptionQuote(bp, ap, bs, as_, ts)

    val greeks = {
      val g = c.downField("greeks")
      val delta = g.get[Double]("delta").toOption
      val gamma = g.get[Double]("gamma").toOption
      val theta = g.get[Double]("theta").toOption
      val vega  = g.get[Double]("vega").toOption
      val rho   = g.get[Double]("rho").toOption
      if delta.isDefined || gamma.isDefined then Some(OptionGreeks(delta, gamma, theta, vega, rho))
      else None
    }

    val iv = c.downField("impliedVolatility").as[Double].toOption
      .orElse(c.downField("greeks").downField("implied_volatility").as[Double].toOption)

    OptionSnapshot(contractSymbol, trade, quote, greeks, iv)

  def close(): Unit = backend.close()
