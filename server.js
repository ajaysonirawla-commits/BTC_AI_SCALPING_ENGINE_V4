require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: "1mb" }));

// ------------------------------------------------------------
// CONFIG
// ------------------------------------------------------------

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || "0.0.0.0";

const OKX_BASE = "https://www.okx.com";

const SPOT_INST = "BTC-USDT";
const SWAP_INST = "BTC-USDT-SWAP";

const FRONTEND_DIR = path.join(__dirname, "frontend");

// ------------------------------------------------------------
// STATE / CACHE
// ------------------------------------------------------------

const cache = {
  ticker: null,
  candles: {},
  oi: null,
  funding: null,
  index: null,
  fearGreed: null,
  dashboard: null,
  lastUpdate: 0
};

const CACHE_MS = 8000;
const CANDLE_CACHE_MS = 10000;

const signalHistory = [];
const MAX_HISTORY = 100;

let lastGoodTicker = null;
let lastGoodOI = null;
let lastGoodFunding = null;

// ------------------------------------------------------------
// EXPRESS STATIC
// ------------------------------------------------------------

app.use(express.static(FRONTEND_DIR));

// ------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------

function nowISO() {
  return new Date().toISOString();
}

function num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function round(v, decimals = 2) {
  if (!Number.isFinite(v)) return null;
  const p = Math.pow(10, decimals);
  return Math.round(v * p) / p;
}

async function fetchJSON(url, options = {}) {
  const controller = new AbortController();

  const timeout = setTimeout(() => {
    controller.abort();
  }, options.timeout || 8000);

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Accept": "application/json",
        "User-Agent": "BTC-AI-SCALPING-ENGINE-V4/1.0"
      },
      signal: controller.signal
    });

    const text = await response.text();

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} from ${new URL(url).hostname}`);
    }

    let json;

    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(`Non-JSON response from ${new URL(url).hostname}`);
    }

    return json;
  } finally {
    clearTimeout(timeout);
  }
}

// ------------------------------------------------------------
// OKX TICKER
// ------------------------------------------------------------

async function getTicker() {
  const url =
    `${OKX_BASE}/api/v5/market/ticker?instId=${encodeURIComponent(SWAP_INST)}`;

  try {
    const json = await fetchJSON(url);

    if (json.code !== "0" || !Array.isArray(json.data) || !json.data[0]) {
      throw new Error(json.msg || "OKX ticker unavailable");
    }

    const d = json.data[0];

    const ticker = {
      symbol: SWAP_INST,
      lastPrice: num(d.last),
      bid: num(d.bidPx),
      ask: num(d.askPx),
      high24h: num(d.high24h),
      low24h: num(d.low24h),
      open24h: num(d.open24h),
      volume24h: num(d.vol24h),
      volumeCcy24h: num(d.volCcy24h),
      timestamp: num(d.ts),
      source: "OKX public"
    };

    lastGoodTicker = ticker;
    cache.ticker = {
      data: ticker,
      time: Date.now()
    };

    return ticker;
  } catch (error) {
    if (lastGoodTicker) {
      return {
        ...lastGoodTicker,
        stale: true,
        error: error.message
      };
    }

    throw error;
  }
}

// ------------------------------------------------------------
// OKX CANDLES
// ------------------------------------------------------------

function normalizeBar(bar) {
  const allowed = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D"
  ];

  return allowed.includes(bar) ? bar : "5m";
}

async function getCandles(bar = "5m", limit = 100) {
  bar = normalizeBar(bar);
  limit = clamp(Number(limit) || 100, 20, 300);

  const key = `${bar}_${limit}`;

  if (
    cache.candles[key] &&
    Date.now() - cache.candles[key].time < CANDLE_CACHE_MS
  ) {
    return cache.candles[key].data;
  }

  const url =
    `${OKX_BASE}/api/v5/market/candles` +
    `?instId=${encodeURIComponent(SPOT_INST)}` +
    `&bar=${encodeURIComponent(bar)}` +
    `&limit=${limit}`;

  const json = await fetchJSON(url);

  if (json.code !== "0" || !Array.isArray(json.data)) {
    throw new Error(json.msg || "OKX candles unavailable");
  }

  const candles = json.data
    .map(row => ({
      timestamp: num(row[0]),
      open: num(row[1]),
      high: num(row[2]),
      low: num(row[3]),
      close: num(row[4]),
      volume: num(row[5], 0),
      volumeCurrency: num(row[6], 0),
      volumeQuote: num(row[7], 0),
      confirmed: String(row[8]) === "1"
    }))
    .filter(x =>
      Number.isFinite(x.timestamp) &&
      Number.isFinite(x.open) &&
      Number.isFinite(x.high) &&
      Number.isFinite(x.low) &&
      Number.isFinite(x.close)
    )
    .sort((a, b) => a.timestamp - b.timestamp);

  cache.candles[key] = {
    data: candles,
    time: Date.now()
  };

  return candles;
}

// ------------------------------------------------------------
// OPEN INTEREST
// ------------------------------------------------------------

async function getOpenInterest() {
  const url =
    `${OKX_BASE}/api/v5/public/open-interest` +
    `?instType=SWAP` +
    `&instId=${encodeURIComponent(SWAP_INST)}`;

  try {
    const json = await fetchJSON(url);

    if (json.code !== "0" || !Array.isArray(json.data) || !json.data[0]) {
      throw new Error(json.msg || "OKX Open Interest unavailable");
    }

    const d = json.data[0];

    const oi = {
      symbol: SWAP_INST,
      oi: num(d.oi),
      oiCcy: num(d.oiCcy),
      timestamp: num(d.ts),
      source: "OKX public"
    };

    lastGoodOI = oi;

    cache.oi = {
      data: oi,
      time: Date.now()
    };

    return oi;
  } catch (error) {
    if (lastGoodOI) {
      return {
        ...lastGoodOI,
        stale: true,
        error: error.message
      };
    }

    throw error;
  }
}

// ------------------------------------------------------------
// FUNDING
// ------------------------------------------------------------

async function getFunding() {
  const url =
    `${OKX_BASE}/api/v5/public/funding-rate` +
    `?instId=${encodeURIComponent(SWAP_INST)}`;

  try {
    const json = await fetchJSON(url);

    if (json.code !== "0" || !Array.isArray(json.data) || !json.data[0]) {
      throw new Error(json.msg || "OKX Funding unavailable");
    }

    const d = json.data[0];

    const funding = {
      symbol: SWAP_INST,
      fundingRate: num(d.fundingRate),
      fundingRatePercent:
        num(d.fundingRate) !== null
          ? num(d.fundingRate) * 100
          : null,
      nextFundingTime: num(d.nextFundingTime),
      fundingTime: num(d.fundingTime),
      source: "OKX public"
    };

    lastGoodFunding = funding;

    cache.funding = {
      data: funding,
      time: Date.now()
    };

    return funding;
  } catch (error) {
    if (lastGoodFunding) {
      return {
        ...lastGoodFunding,
        stale: true,
        error: error.message
      };
    }

    throw error;
  }
}

// ------------------------------------------------------------
// INDEX PRICE
// ------------------------------------------------------------

async function getIndexPrice() {
  const url =
    `${OKX_BASE}/api/v5/market/index-tickers` +
    `?instId=BTC-USDT`;

  try {
    const json = await fetchJSON(url);

    if (json.code !== "0" || !Array.isArray(json.data) || !json.data[0]) {
      throw new Error(json.msg || "Index price unavailable");
    }

    const d = json.data[0];

    const result = {
      indexPrice: num(d.idxPx),
      timestamp: num(d.ts),
      source: "OKX public"
    };

    cache.index = {
      data: result,
      time: Date.now()
    };

    return result;
  } catch {
    return cache.index ? cache.index.data : null;
  }
}

// ------------------------------------------------------------
// TECHNICAL INDICATORS
// ------------------------------------------------------------

function calculateEMA(values, period) {
  if (!values.length) return null;

  const k = 2 / (period + 1);

  let ema = values[0];

  for (let i = 1; i < values.length; i++) {
    ema = values[i] * k + ema * (1 - k);
  }

  return ema;
}

function calculateRSI(values, period = 14) {
  if (values.length < period + 1) return null;

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const diff = values[i] - values[i - 1];

    if (diff >= 0) gains += diff;
    else losses += Math.abs(diff);
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (let i = period + 1; i < values.length; i++) {
    const diff = values[i] - values[i - 1];

    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;

    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
  }

  if (avgLoss === 0) return 100;

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

function calculateVWAP(candles) {
  if (!candles.length) return null;

  let cumulativePV = 0;
  let cumulativeVolume = 0;

  for (const c of candles) {
    const typical = (c.high + c.low + c.close) / 3;

    cumulativePV += typical * (c.volume || 0);
    cumulativeVolume += c.volume || 0;
  }

  if (cumulativeVolume === 0) return null;

  return cumulativePV / cumulativeVolume;
}

function calculateATR(candles, period = 14) {
  if (candles.length < period + 1) return null;

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const prev = candles[i - 1];

    const tr = Math.max(
      c.high - c.low,
      Math.abs(c.high - prev.close),
      Math.abs(c.low - prev.close)
    );

    trs.push(tr);
  }

  const recent = trs.slice(-period);

  return recent.reduce((a, b) => a + b, 0) / recent.length;
}

function calculateVolumeRatio(candles, period = 20) {
  if (candles.length < period + 1) return null;

  const recent = candles.slice(-period - 1);

  const current = recent[recent.length - 1].volume || 0;

  const previous = recent
    .slice(0, -1)
    .map(x => x.volume || 0);

  const avg =
    previous.reduce((a, b) => a + b, 0) /
    Math.max(previous.length, 1);

  if (!avg) return null;

  return current / avg;
}

// ------------------------------------------------------------
// MARKET ANALYSIS
// ------------------------------------------------------------

function buildTechnical(candles, ticker) {
  const closes = candles.map(x => x.close);

  const price =
    ticker?.lastPrice ??
    closes[closes.length - 1] ??
    null;

  const rsi = calculateRSI(closes, 14);
  const ema20 = calculateEMA(closes, 20);
  const ema50 = calculateEMA(closes, 50);
  const vwap = calculateVWAP(candles);
  const atr = calculateATR(candles, 14);
  const volumeRatio = calculateVolumeRatio(candles, 20);

  let trend = "MIXED";

  if (
    price !== null &&
    ema20 !== null &&
    ema50 !== null
  ) {
    if (price > ema20 && ema20 > ema50) {
      trend = "BULLISH";
    } else if (price < ema20 && ema20 < ema50) {
      trend = "BEARISH";
    }
  }

  let vwapBias = "NEUTRAL";

  if (price !== null && vwap !== null) {
    if (price > vwap) vwapBias = "ABOVE VWAP";
    if (price < vwap) vwapBias = "BELOW VWAP";
  }

  return {
    price: round(price, 2),
    rsi: round(rsi, 2),
    ema20: round(ema20, 2),
    ema50: round(ema50, 2),
    vwap: round(vwap, 2),
    atr: round(atr, 2),
    volumeRatio: round(volumeRatio, 3),
    trend,
    vwapBias
  };
}

// ------------------------------------------------------------
// SIGNAL ENGINE 0-100
// ------------------------------------------------------------

function buildSignal(technical, oi, funding, ticker) {
  const components = {};

  let bullish = 0;
  let bearish = 0;

  // PRICE STRUCTURE — 15
  let priceStructure = 7.5;

  if (technical.trend === "BULLISH") {
    priceStructure = 15;
    bullish += 15;
  } else if (technical.trend === "BEARISH") {
    priceStructure = 0;
    bearish += 15;
  } else {
    bullish += 7.5;
    bearish += 7.5;
  }

  components.priceStructure = {
    score: round(priceStructure, 1),
    label: technical.trend
  };

  // RSI — 10
  let rsiScore = 5;

  if (technical.rsi !== null) {
    if (technical.rsi >= 55 && technical.rsi < 70) {
      bullish += 10;
      rsiScore = 10;
    } else if (technical.rsi <= 45 && technical.rsi > 30) {
      bearish += 10;
      rsiScore = 0;
    } else if (technical.rsi >= 70) {
      bullish += 5;
      rsiScore = 5;
    } else if (technical.rsi <= 30) {
      bearish += 5;
      rsiScore = 5;
    } else {
      bullish += 5;
      bearish += 5;
    }
  } else {
    bullish += 5;
    bearish += 5;
  }

  components.rsi = {
    score: rsiScore,
    value: technical.rsi
  };

  // EMA + VWAP — 10
  let emaVwapScore = 5;

  if (
    technical.price !== null &&
    technical.ema20 !== null &&
    technical.vwap !== null
  ) {
    if (
      technical.price > technical.ema20 &&
      technical.price > technical.vwap
    ) {
      bullish += 10;
      emaVwapScore = 10;
    } else if (
      technical.price < technical.ema20 &&
      technical.price < technical.vwap
    ) {
      bearish += 10;
      emaVwapScore = 0;
    } else {
      bullish += 5;
      bearish += 5;
    }
  } else {
    bullish += 5;
    bearish += 5;
  }

  components.emaVwap = {
    score: emaVwapScore,
    label: technical.vwapBias
  };

  // VOLUME — 10
  let volumeScore = 5;

  if (technical.volumeRatio !== null) {
    if (technical.volumeRatio >= 1.5) {
      if (technical.trend === "BULLISH") {
        bullish += 10;
        volumeScore = 10;
      } else if (technical.trend === "BEARISH") {
        bearish += 10;
        volumeScore = 0;
      } else {
        bullish += 5;
        bearish += 5;
      }
    } else {
      bullish += 5;
      bearish += 5;
    }
  } else {
    bullish += 5;
    bearish += 5;
  }

  components.volume = {
    score: volumeScore,
    ratio: technical.volumeRatio
  };

  // OPEN INTEREST — 15
  // Without historical OI we deliberately do not fake direction.
  let oiScore = 7.5;

  if (oi && oi.oi !== null) {
    bullish += 7.5;
    bearish += 7.5;
    oiScore = 7.5;
  } else {
    bullish += 7.5;
    bearish += 7.5;
  }

  components.openInterest = {
    score: oiScore,
    value: oi?.oi ?? null,
    status: oi ? "LIVE" : "UNAVAILABLE"
  };

  // FUNDING — 5
  let fundingScore = 2.5;

  if (funding && funding.fundingRate !== null) {
    const rate = funding.fundingRate;

    if (rate > 0.0005) {
      bearish += 5;
      fundingScore = 0;
    } else if (rate < -0.0005) {
      bullish += 5;
      fundingScore = 5;
    } else {
      bullish += 2.5;
      bearish += 2.5;
    }
  } else {
    bullish += 2.5;
    bearish += 2.5;
  }

  components.funding = {
    score: fundingScore,
    rate: funding?.fundingRate ?? null,
    percent: funding?.fundingRatePercent ?? null
  };

  // LIQUIDATION / DERIVATIVES — 10
  // No fake liquidation data.
  const liquidationScore = 5;

  bullish += 5;
  bearish += 5;

  components.liquidation = {
    score: liquidationScore,
    label: "NO LIVE LIQUIDATION FEED"
  };

  // LARGE TRADES / WHALE — 10
  const whaleScore = 5;

  bullish += 5;
  bearish += 5;

  components.largeTrades = {
    score: whaleScore,
    label: "NO LIVE WHALE FEED"
  };

  // TOTAL
  const rawBull = bullish;
  const rawBear = bearish;

  const total = 75;

  const bullPercent = (rawBull / total) * 100;
  const bearPercent = (rawBear / total) * 100;

  let score = Math.round(
    50 + (bullPercent - bearPercent) / 2
  );

  score = clamp(score, 0, 100);

  let action = "WAIT";
  let label = "WAIT";

  if (score >= 65) {
    action = "LONG";
    label = "BULLISH";
  } else if (score <= 35) {
    action = "SHORT";
    label = "BEARISH";
  }

  const confidence = Math.round(
    Math.abs(score - 50) * 2
  );

  // ENTRY / SL / TARGETS
  let entry = technical.price;
  let stopLoss = null;
  let target1 = null;
  let target2 = null;

  const atr =
    technical.atr ||
    (technical.price ? technical.price * 0.003 : null);

  if (entry !== null && atr !== null) {
    if (action === "LONG") {
      stopLoss = entry - atr * 1.2;
      target1 = entry + atr * 1.5;
      target2 = entry + atr * 2.5;
    } else if (action === "SHORT") {
      stopLoss = entry + atr * 1.2;
      target1 = entry - atr * 1.5;
      target2 = entry - atr * 2.5;
    }
  }

  const dataCoverage = {
    price: Boolean(ticker?.lastPrice),
    candles: true,
    openInterest: Boolean(oi),
    funding: Boolean(funding),
    liquidation: false,
    largeTrades: false
  };

  const coverage = Math.round(
    (
      Object.values(dataCoverage)
        .filter(Boolean).length /
      Object.values(dataCoverage).length
    ) * 100
  );

  // If essential derivatives data is missing, don't generate a
  // strong directional signal.
  if (!oi || !funding) {
    action = "WAIT";
    label = "WAIT";
  }

  return {
    score,
    confidence,
    action,
    label,

    entry: round(entry, 2),
    stopLoss: round(stopLoss, 2),
    target1: round(target1, 2),
    target2: round(target2, 2),

    holdingWindow:
      action === "LONG" || action === "SHORT"
        ? "5–30 min"
        : "Wait for confirmation",

    coverage,

    bullScore: round(rawBull, 1),
    bearScore: round(rawBear, 1),

    components,

    reason:
      action === "WAIT"
        ? "Live confirmation/data coverage is insufficient."
        : `${label} setup based on price structure, RSI, EMA/VWAP and volume.`,

    dataCoverage
  };
}

// ------------------------------------------------------------
// FEAR & GREED
// ------------------------------------------------------------

async function getFearGreed() {
  if (
    cache.fearGreed &&
    Date.now() - cache.fearGreed.time < 60000
  ) {
    return cache.fearGreed.data;
  }

  try {
    const json = await fetchJSON(
      "https://api.alternative.me/fng/?limit=1"
    );

    const d = json?.data?.[0];

    if (!d) throw new Error("Fear & Greed unavailable");

    const result = {
      value: num(d.value),
      classification: d.value_classification || null,
      timestamp: num(d.timestamp),
      source: "Alternative.me"
    };

    cache.fearGreed = {
      data: result,
      time: Date.now()
    };

    return result;
  } catch {
    return null;
  }
}

// ------------------------------------------------------------
// DASHBOARD
// ------------------------------------------------------------

async function buildDashboard() {
  const errors = {};

  let ticker = null;
  let candles = [];
  let oi = null;
  let funding = null;
  let index = null;
  let fearGreed = null;

  try {
    ticker = await getTicker();
  } catch (e) {
    errors.marketData = e.message;
  }

  try {
    candles = await getCandles("5m", 100);
  } catch (e) {
    errors.candles = e.message;
  }

  try {
    oi = await getOpenInterest();
  } catch (e) {
    errors.openInterest = e.message;
  }

  try {
    funding = await getFunding();
  } catch (e) {
    errors.funding = e.message;
  }

  try {
    index = await getIndexPrice();
  } catch {
    index = null;
  }

  try {
    fearGreed = await getFearGreed();
  } catch {
    fearGreed = null;
  }

  if (!ticker && !candles.length) {
    return {
      ok: false,
      timestamp: nowISO(),
      source: "OKX public",
      error: "Live market data unavailable",
      errors
    };
  }

  const technical = buildTechnical(candles, ticker);

  const signal = buildSignal(
    technical,
    oi,
    funding,
    ticker
  );

  const dashboard = {
    ok: true,

    timestamp: nowISO(),

    source: "OKX public",

    btc: {
      symbol: "BTCUSDT",
      price: technical.price,
      markPrice: technical.price,
      indexPrice: index?.indexPrice ?? null,
      change24h:
        ticker?.open24h && ticker?.lastPrice
          ? round(
              ((ticker.lastPrice - ticker.open24h) /
                ticker.open24h) *
                100,
              3
            )
          : null,
      high24h: ticker?.high24h ?? null,
      low24h: ticker?.low24h ?? null,
      volume24h: ticker?.volume24h ?? null,
      turnover24h: ticker?.volumeCcy24h ?? null
    },

    timeframes: {
      "1m": {
        status: "AVAILABLE",
        endpoint: "/api/candles?bar=1m"
      },
      "5m": {
        status: "AVAILABLE",
        endpoint: "/api/candles?bar=5m"
      },
      "15m": {
        status: "AVAILABLE",
        endpoint: "/api/candles?bar=15m"
      },
      "30m": {
        status: "AVAILABLE",
        endpoint: "/api/candles?bar=30m"
      },
      "1h": {
        status: "AVAILABLE",
        endpoint: "/api/candles?bar=1H"
      }
    },

    technical,

    openInterest: {
      btc: oi?.oi ?? null,
      usd: oi?.oiCcy ?? null,
      timestamp: oi?.timestamp ?? null,
      source: oi?.source ?? null,
      status: oi ? "LIVE" : "UNAVAILABLE"
    },

    funding: {
      rate: funding?.fundingRate ?? null,
      ratePercent:
        funding?.fundingRatePercent ?? null,
      nextFundingTime:
        funding?.nextFundingTime ?? null,
      status: funding ? "LIVE" : "UNAVAILABLE"
    },

    liquidation: {
      total: null,
      long1h: null,
      short1h: null,
      bias: "UNAVAILABLE",
      source: "No liquidation API connected"
    },

    largeTrades: {
      count: null,
      buyUsd: null,
      sellUsd: null,
      netUsd: null,
      bias: "UNAVAILABLE",
      source: "No whale feed connected"
    },

    fearGreed,

    macro: {
      source: "Not connected",
      status: "OPTIONAL"
    },

    sessions: {
      india: "ACTIVE CHECK",
      usa: "ACTIVE CHECK",
      asia: "ACTIVE CHECK",
      note: "Regular-session clock display only"
    },

    signal,

    weights: {
      priceStructure: 15,
      rsi: 10,
      emaVwap: 10,
      volume: 10,
      openInterest: 15,
      funding: 5,
      liquidation: 10,
      largeTrades: 10,
      fearGreed: 5
    },

    errors
  };

  cache.dashboard = {
    data: dashboard,
    time: Date.now()
  };

  return dashboard;
}

// ------------------------------------------------------------
// API: HEALTH
// ------------------------------------------------------------

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "BTC/USD AI SCALPING ENGINE V4",
    time: nowISO(),
    websocket: {
      connected: wss.clients.size > 0,
      clients: wss.clients.size
    },
    provider: "OKX public",
    uptime: process.uptime(),
    node: process.version
  });
});

// ------------------------------------------------------------
// API: PRICE
// ------------------------------------------------------------

app.get("/api/price", async (req, res) => {
  try {
    const ticker = await getTicker();

    res.json({
      symbol: "BTCUSDT",
      lastPrice: ticker.lastPrice,
      markPrice: ticker.lastPrice,
      indexPrice: cache.index?.data?.indexPrice ?? null,
      high24h: ticker.high24h,
      low24h: ticker.low24h,
      volume24h: ticker.volume24h,
      turnover24h: ticker.volumeCcy24h,
      source: ticker.source,
      stale: Boolean(ticker.stale),
      error: ticker.error || null
    });
  } catch (error) {
    res.status(200).json({
      error: error.message
    });
  }
});

// ------------------------------------------------------------
// API: CANDLES
// ------------------------------------------------------------

app.get("/api/candles", async (req, res) => {
  try {
    const bar = normalizeBar(req.query.bar || "5m");
    const limit = clamp(
      Number(req.query.limit) || 100,
      20,
      300
    );

    const candles = await getCandles(bar, limit);

    res.json({
      ok: true,
      symbol: SPOT_INST,
      bar,
      count: candles.length,
      source: "OKX public",
      candles
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      error: error.message
    });
  }
});

// ------------------------------------------------------------
// API: OPEN INTEREST
// ------------------------------------------------------------

app.get("/api/oi", async (req, res) => {
  try {
    const oi = await getOpenInterest();

    res.json({
      ok: true,
      symbol: "BTCUSDT",
      oi: oi.oi,
      oiCcy: oi.oiCcy,
      timestamp: oi.timestamp,
      source: oi.source,
      stale: Boolean(oi.stale),
      error: oi.error || null
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      error: error.message
    });
  }
});

// ------------------------------------------------------------
// API: FUNDING
// ------------------------------------------------------------

app.get("/api/funding", async (req, res) => {
  try {
    const funding = await getFunding();

    res.json({
      ok: true,
      symbol: "BTCUSDT",
      fundingRate: funding.fundingRate,
      fundingRatePercent:
        funding.fundingRatePercent,
      nextFundingTime:
        funding.nextFundingTime,
      fundingTime:
        funding.fundingTime,
      source: funding.source,
      stale: Boolean(funding.stale),
      error: funding.error || null
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      error: error.message
    });
  }
});

// ------------------------------------------------------------
// API: DASHBOARD
// ------------------------------------------------------------

app.get("/api/dashboard", async (req, res) => {
  try {
    const dashboard = await buildDashboard();

    res.json(dashboard);

    // Broadcast live dashboard to WebSocket clients
    broadcast({
      type: "dashboard",
      data: dashboard
    });

    recordSignal(dashboard);

  } catch (error) {
    res.status(200).json({
      ok: false,
      timestamp: nowISO(),
      error: error.message
    });
  }
});

// ------------------------------------------------------------
// SIGNAL HISTORY
// ------------------------------------------------------------

function recordSignal(dashboard) {
  if (!dashboard?.signal) return;

  const s = dashboard.signal;

  const item = {
    id: Date.now().toString(),
    timestamp: dashboard.timestamp,
    action: s.action,
    label: s.label,
    score: s.score,
    confidence: s.confidence,
    entry: s.entry,
    stopLoss: s.stopLoss,
    target1: s.target1,
    target2: s.target2,
    read: false
  };

  const last = signalHistory[0];

  if (
    last &&
    last.action === item.action &&
    last.score === item.score
  ) {
    return;
  }

  signalHistory.unshift(item);

  while (signalHistory.length > MAX_HISTORY) {
    signalHistory.pop();
  }
}

app.get("/api/history", (req, res) => {
  res.json({
    ok: true,
    count: signalHistory.length,
    history: signalHistory
  });
});

app.post("/api/history/:id/read", (req, res) => {
  const item = signalHistory.find(
    x => x.id === req.params.id
  );

  if (!item) {
    return res.status(404).json({
      ok: false,
      error: "Signal not found"
    });
  }

  item.read = true;

  res.json({
    ok: true,
    item
  });
});

app.post("/api/history/read-all", (req, res) => {
  signalHistory.forEach(x => {
    x.read = true;
  });

  res.json({
    ok: true
  });
});

// ------------------------------------------------------------
// ALERTS
// ------------------------------------------------------------

app.get("/api/alerts", (req, res) => {
  const unread = signalHistory.filter(
    x => !x.read
  );

  res.json({
    ok: true,
    count: unread.length,
    alerts: unread
  });
});

// ------------------------------------------------------------
// WEBSOCKET
// ------------------------------------------------------------

function broadcast(message) {
  const payload = JSON.stringify(message);

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(payload);
      } catch {}
    }
  });
}

wss.on("connection", async ws => {
  try {
    ws.send(
      JSON.stringify({
        type: "connected",
        time: nowISO(),
        provider: "OKX public"
      })
    );

    if (cache.dashboard?.data) {
      ws.send(
        JSON.stringify({
          type: "dashboard",
          data: cache.dashboard.data
        })
      );
    }
  } catch {}
});

// ------------------------------------------------------------
// PERIODIC LIVE UPDATE
// ------------------------------------------------------------

let updateRunning = false;

async function periodicUpdate() {
  if (updateRunning) return;

  updateRunning = true;

  try {
    const dashboard = await buildDashboard();

    recordSignal(dashboard);

    broadcast({
      type: "dashboard",
      data: dashboard
    });

  } catch (error) {
    broadcast({
      type: "error",
      error: error.message,
      time: nowISO()
    });
  } finally {
    updateRunning = false;
  }
}

// Every 10 seconds
setInterval(periodicUpdate, 10000);

// ------------------------------------------------------------
// FRONTEND FALLBACK
// ------------------------------------------------------------

app.get("*", (req, res) => {
  if (req.path.startsWith("/api/")) {
    return res.status(404).json({
      ok: false,
      error: "API route not found"
    });
  }

  res.sendFile(
    path.join(FRONTEND_DIR, "index.html")
  );
});

// ------------------------------------------------------------
// ERROR HANDLER
// ------------------------------------------------------------

app.use((err, req, res, next) => {
  console.error("SERVER ERROR:", err);

  res.status(500).json({
    ok: false,
    error: "Internal server error"
  });
});

// ------------------------------------------------------------
// START
// ------------------------------------------------------------

server.listen(PORT, HOST, () => {
  console.log("==============================================");
  console.log(" BTC/USD AI SCALPING ENGINE V4");
  console.log("==============================================");
  console.log(`Server: http://${HOST}:${PORT}`);
  console.log(`Provider: OKX Public API`);
  console.log(`BTC Spot: ${SPOT_INST}`);
  console.log(`BTC Swap: ${SWAP_INST}`);
  console.log(`Node: ${process.version}`);
  console.log("==============================================");
});
