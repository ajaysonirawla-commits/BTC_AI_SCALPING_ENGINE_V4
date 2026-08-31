require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'frontend')));

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const SYMBOL = process.env.SYMBOL || 'BTCUSDT';

const SPOT_BASES = [
  'https://api.binance.com',
  'https://api1.binance.com',
  'https://api2.binance.com',
  'https://api3.binance.com',
  'https://api4.binance.com'
];

const FUTURES_BASES = [
  'https://fapi.binance.com',
  'https://fapi1.binance.com',
  'https://fapi2.binance.com',
  'https://fapi3.binance.com',
  'https://fapi4.binance.com'
];

const history = [];
const alerts = [];

let lastDashboard = null;
let lastGoodPrice = null;

function num(v, fallback = null) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(v, min, max) {
  return Math.min(max, Math.max(min, v));
}

function avg(arr) {
  const a = arr.filter(Number.isFinite);
  return a.length
    ? a.reduce((x, y) => x + y, 0) / a.length
    : null;
}

function fmt(v, d = 2) {
  return Number.isFinite(v) ? Number(v.toFixed(d)) : null;
}

async function fetchJson(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'User-Agent': 'BTC-AI-SCALPING-ENGINE-V5/1.0',
        'Accept': 'application/json,text/plain,*/*',
        ...(options.headers || {})
      }
    });

    const text = await res.text();

    if (!res.ok) {
      throw new Error(
        `HTTP ${res.status} from ${new URL(url).hostname}`
      );
    }

    try {
      return JSON.parse(text);
    } catch {
      throw new Error(
        `Non-JSON response from ${new URL(url).hostname}`
      );
    }
  } finally {
    clearTimeout(timer);
  }
}

async function tryBases(bases, endpoint) {
  let lastError = null;

  for (const base of bases) {
    try {
      return await fetchJson(`${base}${endpoint}`);
    } catch (e) {
      lastError = e;
    }
  }

  throw lastError || new Error('All market data sources failed');
}

async function getSpot24h() {
  return tryBases(
    SPOT_BASES,
    `/api/v3/ticker/24hr?symbol=${encodeURIComponent(SYMBOL)}`
  );
}

async function getFutures24h() {
  return tryBases(
    FUTURES_BASES,
    `/fapi/v1/ticker/24hr?symbol=${encodeURIComponent(SYMBOL)}`
  );
}

async function getSpotKlines(interval = '5m', limit = 120) {
  limit = clamp(
    Math.floor(num(limit, 120)),
    20,
    500
  );

  return tryBases(
    SPOT_BASES,
    `/api/v3/klines?symbol=${SYMBOL}&interval=${encodeURIComponent(interval)}&limit=${limit}`
  );
}

async function getOpenInterest() {
  return tryBases(
    FUTURES_BASES,
    `/fapi/v1/openInterest?symbol=${SYMBOL}`
  );
}

async function getFunding() {
  return tryBases(
    FUTURES_BASES,
    `/fapi/v1/fundingRate?symbol=${SYMBOL}&limit=1`
  );
}

async function getGlobalLongShort() {
  try {
    return await tryBases(
      FUTURES_BASES,
      `/futures/data/globalLongShortAccountRatio?symbol=${SYMBOL}&period=5m&limit=1`
    );
  } catch {
    return null;
  }
}

function candlesFromBinance(rows) {
  return (Array.isArray(rows) ? rows : [])
    .map(k => ({
      time: Number(k[0]),
      open: Number(k[1]),
      high: Number(k[2]),
      low: Number(k[3]),
      close: Number(k[4]),
      volume: Number(k[5])
    }))
    .filter(x =>
      [
        x.open,
        x.high,
        x.low,
        x.close,
        x.volume
      ].every(Number.isFinite)
    );
}

function ema(values, period) {
  if (values.length < period) return null;

  let e = avg(values.slice(0, period));
  const k = 2 / (period + 1);

  for (let i = period; i < values.length; i++) {
    e = values[i] * k + e * (1 - k);
  }

  return e;
}

function rsi(values, period = 14) {
  if (values.length < period + 1) return null;

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const d = values[i] - values[i - 1];

    if (d >= 0) {
      gains += d;
    } else {
      losses -= d;
    }
  }

  let ag = gains / period;
  let al = losses / period;

  for (let i = period + 1; i < values.length; i++) {
    const d = values[i] - values[i - 1];

    ag =
      ((ag * (period - 1)) + Math.max(d, 0))
      / period;

    al =
      ((al * (period - 1)) + Math.max(-d, 0))
      / period;
  }

  if (al === 0) return 100;

  return 100 - (100 / (1 + ag / al));
}

function atr(candles, period = 14) {
  if (candles.length < period + 1) return null;

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const p = candles[i - 1];

    trs.push(
      Math.max(
        c.high - c.low,
        Math.abs(c.high - p.close),
        Math.abs(c.low - p.close)
      )
    );
  }

  return avg(trs.slice(-period));
}

function sessionVwap(candles) {
  let pv = 0;
  let vol = 0;

  for (const c of candles) {
    const typical =
      (c.high + c.low + c.close) / 3;

    pv += typical * c.volume;
    vol += c.volume;
  }

  return vol ? pv / vol : null;
}

function volumeRatio(candles, period = 20) {
  if (candles.length < period + 1) return null;

  const current =
    candles[candles.length - 1].volume;

  const base = avg(
    candles
      .slice(-period - 1, -1)
      .map(c => c.volume)
  );

  return base ? current / base : null;
}

function timeframeAnalysis(candles) {
  const closes = candles.map(c => c.close);

  const price = closes.at(-1);

  const e20 = ema(closes, 20);
  const e50 = ema(closes, 50);

  const r = rsi(closes, 14);
  const vwap = sessionVwap(candles);

  const a = atr(candles, 14);
  const vr = volumeRatio(candles, 20);

  let score = 50;

  if (e20 != null) {
    score += price > e20 ? 12 : -12;
  }

  if (e50 != null) {
    score += price > e50 ? 13 : -13;
  }

  if (r != null) {
    if (r > 55 && r < 75) score += 5;
    if (r < 45 && r > 25) score -= 5;
    if (r > 80) score -= 5;
    if (r < 20) score += 5;
  }

  score = clamp(score, 0, 100);

  return {
    price: fmt(price),
    rsi: fmt(r),
    ema20: fmt(e20),
    ema50: fmt(e50),
    vwap: fmt(vwap),
    atr: fmt(a),
    volumeRatio: fmt(vr, 3),

    trend:
      score >= 60
        ? 'BULLISH'
        : score <= 40
          ? 'BEARISH'
          : 'MIXED',

    vwapBias:
      vwap == null
        ? 'UNKNOWN'
        : price > vwap
          ? 'ABOVE VWAP'
          : 'BELOW VWAP',

    score: Math.round(score)
  };
}

function buildSignal(tf, derivatives = {}) {
  const f = tf['5m'] || {};
  const h1 = tf['1h'] || {};
  const m15 = tf['15m'] || {};

  const price = f.price;

  if (!Number.isFinite(price)) {
    return {
      score: 0,
      buyScore: 0,
      sellScore: 0,
      action: 'WAIT',
      label: 'WAIT',
      confidence: 0,
      entry: null,
      stopLoss: null,
      target1: null,
      target2: null,
      holdingWindow: 'WAIT',
      reason: 'Live price unavailable',
      coverage: 0,
      components: {},
      weights: {}
    };
  }

  let buy = 0;
  let sell = 0;

  const components = {};

  const weights = {
    priceStructure: 15,
    rsi: 10,
    emaVwap: 10,
    volume: 10,
    openInterest: 15,
    funding: 5,
    liquidation: 10,
    largeTrades: 10,
    fearGreed: 5
  };

  const ps = clamp(
    (
      (h1.score || 50) * 0.45 +
      (m15.score || 50) * 0.30 +
      (f.score || 50) * 0.25
    ),
    0,
    100
  );

  buy += ps * 0.15;
  sell += (100 - ps) * 0.15;

  components.priceStructure =
    Math.round(ps);

  const r = f.rsi;

  const rScore =
    r == null
      ? 50
      : r <= 35
        ? 75
        : r >= 65
          ? 25
          : 50 + (50 - r);

  buy += rScore * 0.10;
  sell += (100 - rScore) * 0.10;

  components.rsi =
    Math.round(rScore);

  const ev =
    f.ema20 != null &&
    f.ema50 != null
      ? (
          f.price > f.ema20 &&
          f.ema20 > f.ema50
            ? 85
            : f.price < f.ema20 &&
              f.ema20 < f.ema50
              ? 15
              : 50
        )
      : 50;

  const vw =
    f.vwapBias === 'ABOVE VWAP'
      ? 70
      : f.vwapBias === 'BELOW VWAP'
        ? 30
        : 50;

  const emaVwap =
    (ev + vw) / 2;

  buy += emaVwap * 0.10;
  sell += (100 - emaVwap) * 0.10;

  components.emaVwap =
    Math.round(emaVwap);

  const vr = f.volumeRatio;

  const volScore =
    vr == null
      ? 50
      : clamp(
          50 + (vr - 1) * 35,
          20,
          90
        );

  buy += volScore * 0.10;
  sell += (100 - volScore) * 0.10;

  components.volume =
    Math.round(volScore);

  const oiScore =
    derivatives.oi != null
      ? 60
      : 50;

  const fundScore =
    derivatives.funding != null
      ? clamp(
          Math.round(
            50 -
            derivatives.funding * 100000
          ),
          10,
          90
        )
      : 50;

  const lsScore =
    derivatives.longShort != null
      ? clamp(
          Math.round(
            50 +
            (derivatives.longShort - 1) * 80
          ),
          10,
          90
        )
      : 50;

  components.openInterest = oiScore;
  components.funding = fundScore;
  components.liquidation = 50;
  components.largeTrades = lsScore;
  components.fearGreed = 50;

  buy += oiScore * 0.15;
  buy += fundScore * 0.05;
  buy += 50 * 0.10;
  buy += lsScore * 0.10;
  buy += 50 * 0.05;

  sell += (100 - oiScore) * 0.15;
  sell += (100 - fundScore) * 0.05;
  sell += 50 * 0.10;
  sell += (100 - lsScore) * 0.10;
  sell += 50 * 0.05;

  const buyScore =
    Math.round(clamp(buy, 0, 100));

  const sellScore =
    Math.round(clamp(sell, 0, 100));

  const score =
    Math.max(buyScore, sellScore);

  const action =
    score >= 68
      ? (
          buyScore > sellScore
            ? 'LONG'
            : 'SHORT'
        )
      : 'WAIT';

  const atrVal =
    f.atr || price * 0.002;

  let stopLoss = null;
  let target1 = null;
  let target2 = null;

  if (action === 'LONG') {
    stopLoss =
      price - atrVal * 1.2;

    target1 =
      price + atrVal * 1.5;

    target2 =
      price + atrVal * 2.5;
  }

  if (action === 'SHORT') {
    stopLoss =
      price + atrVal * 1.2;

    target1 =
      price - atrVal * 1.5;

    target2 =
      price - atrVal * 2.5;
  }

  const coverage =
    Math.round(
      Object.values(components)
        .filter(v => v !== 50)
        .length
      / 9 * 100
    );

  return {
    score,
    buyScore,
    sellScore,

    action,

    label:
      action === 'LONG'
        ? 'BUY'
        : action === 'SHORT'
          ? 'SELL'
          : 'WAIT',

    confidence: score,

    coverage,

    entry: fmt(price),
    stopLoss: fmt(stopLoss),
    target1: fmt(target1),
    target2: fmt(target2),

    holdingWindow:
      action === 'WAIT'
        ? 'Wait for confirmation'
        : '5–30 min',

    reason:
      action === 'WAIT'
        ? 'Trend/confirmation not strong enough'
        : 'Multi-factor technical alignment',

    components,
    weights
  };
}

async function buildDashboard() {
  const intervals = [
    '1m',
    '5m',
    '15m',
    '1h'
  ];

  const pairs =
    await Promise.all(
      intervals.map(
        async interval => {
          try {
            const rows =
              await getSpotKlines(
                interval,
                120
              );

            return [
              interval,
              timeframeAnalysis(
                candlesFromBinance(rows)
              )
            ];
          } catch {
            return [
              interval,
              {}
            ];
          }
        }
      )
    );

  const tf =
    Object.fromEntries(pairs);

  const errors = {};

  let priceData = null;

  try {
    priceData =
      await getSpot24h();
  } catch (e) {

    try {
      priceData =
        await getFutures24h();
    } catch (e2) {
      errors.marketData =
        e2.message;
    }
  }

  let oi = null;
  let funding = null;
  let ratio = null;

  try {
    oi =
      await getOpenInterest();
  } catch (e) {
    errors.openInterest =
      e.message;
  }

  try {
    funding =
      await getFunding();
  } catch (e) {
    errors.funding =
      e.message;
  }

  try {
    ratio =
      await getGlobalLongShort();
  } catch {}

  const price =
    num(
      priceData?.lastPrice,
      tf['5m']?.price
    );

  if (price != null) {
    lastGoodPrice = price;
  }

  const oiValue =
    num(oi?.openInterest);

  const fundingRate =
    num(
      Array.isArray(funding)
        ? funding[0]?.fundingRate
        : funding?.fundingRate
    );

  const longShort =
    num(
      Array.isArray(ratio)
        ? ratio[0]?.longShortRatio
        : ratio?.longShortRatio
    );

  const signal =
    buildSignal(
      tf,
      {
        oi: oiValue,
        funding: fundingRate,
        longShort
      }
    );

  return {

    ok:
      Object.keys(errors).length === 0,

    timestamp:
      new Date().toISOString(),

    source:
      'Binance public',

    btc: {
      symbol: SYMBOL,

      price:
        fmt(price),

      change24h:
        fmt(
          num(
            priceData?.priceChangePercent
          ),
          3
        ),

      volume24h:
        fmt(
          num(priceData?.volume),
          4
        ),

      turnover24h:
        fmt(
          num(priceData?.quoteVolume),
          2
        )
    },

    timeframes: tf,

    openInterest: {
      btc: oiValue,
      usd: null,
      change5m: null,
      change30m: null,
      change1h: null,

      source:
        oiValue != null
          ? 'Binance Futures'
          : 'unavailable'
    },

    funding: {
      rate: fundingRate,

      ratePct:
        fundingRate != null
          ? fmt(
              fundingRate * 100,
              5
            )
          : null,

      nextFundingTime: null,

      source:
        fundingRate != null
          ? 'Binance Futures'
          : 'unavailable'
    },

    liquidation: {
      total: null,
      long1h: null,
      short1h: null,
      events: 0,
      bias: 'UNAVAILABLE'
    },

    largeTrades: {
      count: 0,
      buyUsd: null,
      sellUsd: null,
      netUsd: null,
      bias: 'UNAVAILABLE'
    },

    macro: {
      fearGreed: null,
      indices: {},
      etf: {},
      note:
        'Optional macro feeds not connected'
    },

    fearGreed: {
      value: null,
      classification: 'Unavailable',
      source:
        'Alternative.me public'
    },

    sessions: {
      india: 'ACTIVE CHECK',
      usa: 'ACTIVE CHECK',
      china: 'ACTIVE CHECK',

      note:
        'Session display only'
    },

    signal,

    components:
      signal.components,

    weights:
      signal.weights,

    errors
  };
}

function pushHistory(dashboard) {
  if (!dashboard?.signal) return;

  const s =
    dashboard.signal;

  const item = {
    id:
      `${Date.now()}-${Math.random()
        .toString(36)
        .slice(2, 7)}`,

    timestamp:
      dashboard.timestamp,

    action:
      s.action,

    score:
      s.score,

    buyScore:
      s.buyScore,

    sellScore:
      s.sellScore,

    entry:
      s.entry,

    stopLoss:
      s.stopLoss,

    target1:
      s.target1,

    target2:
      s.target2,

    read: false
  };

  const last =
    history[0];

  if (
    !last ||
    last.action !== item.action ||
    Math.abs(
      last.score - item.score
    ) >= 5
  ) {

    history.unshift(item);

    if (history.length > 100) {
      history.pop();
    }
  }
}

function broadcast(payload) {
  const text =
    JSON.stringify(payload);

  for (
    const client of wss.clients
  ) {

    if (
      client.readyState ===
      WebSocket.OPEN
    ) {
      client.send(text);
    }
  }
}

async function refresh() {
  try {

    const d =
      await buildDashboard();

    lastDashboard = d;

    pushHistory(d);

    broadcast({
      type: 'dashboard',
      data: d
    });

  } catch (e) {

    broadcast({
      type: 'error',
      error: e.message
    });
  }
}

/* =========================
   HEALTH
========================= */

app.get(
  '/api/health',
  (req, res) => {

    res.json({

      ok: true,

      service:
        'BTC/USD AI SCALPING ENGINE V5',

      time:
        new Date().toISOString(),

      websocket: {
        connected:
          wss.clients.size > 0,

        clients:
          wss.clients.size
      },

      provider:
        'Binance public',

      lastError:
        null,

      uptime:
        process.uptime(),

      node:
        process.version
    });
  }
);

/* =========================
   PRICE
========================= */

app.get(
  '/api/price',
  async (req, res) => {

    try {

      const x =
        await getSpot24h();

      res.json({

        symbol: SYMBOL,

        lastPrice:
          num(x.lastPrice),

        markPrice:
          num(x.lastPrice),

        indexPrice:
          null,

        fundingRate:
          null,

        nextFundingTime:
          null,

        volume24h:
          num(x.volume),

        turnover24h:
          num(x.quoteVolume),

        price24hPcnt:
          num(x.priceChangePercent),

        source:
          'Binance'
      });

    } catch (e) {

      try {

        const x =
          await getFutures24h();

        res.json({

          symbol: SYMBOL,

          lastPrice:
            num(x.lastPrice),

          markPrice:
            num(x.lastPrice),

          indexPrice:
            null,

          fundingRate:
            null,

          nextFundingTime:
            null,

          volume24h:
            num(x.volume),

          turnover24h:
            num(x.quoteVolume),

          price24hPcnt:
            num(x.priceChangePercent),

          source:
            'Binance Futures'
        });

      } catch (e2) {

        res.status(502).json({
          error:
            e2.message
        });
      }
    }
  }
);

/* =========================
   CANDLES
========================= */

app.get(
  '/api/candles',
  async (req, res) => {

    const allowed = [
      '1m',
      '3m',
      '5m',
      '15m',
      '30m',
      '1h',
      '4h'
    ];

    const interval =
      allowed.includes(
        req.query.interval
      )
        ? req.query.interval
        : '5m';

    try {

      const rows =
        await getSpotKlines(
          interval,
          req.query.limit || 120
        );

      res.json({

        symbol: SYMBOL,

        interval,

        source:
          'Binance',

        candles:
          candlesFromBinance(rows)
      });

    } catch (e) {

      res.status(502).json({
        error:
          e.message
      });
    }
  }
);

/* =========================
   OPEN INTEREST
========================= */

app.get(
  '/api/oi',
  async (req, res) => {

    try {

      res.json({

        symbol: SYMBOL,

        source:
          'Binance Futures',

        data:
          await getOpenInterest()
      });

    } catch (e) {

      res.status(502).json({
        error:
          e.message
      });
    }
  }
);

/* =========================
   FUNDING
========================= */

app.get(
  '/api/funding',
  async (req, res) => {

    try {

      res.json({

        symbol: SYMBOL,

        source:
          'Binance Futures',

        data:
          await getFunding()
      });

    } catch (e) {

      res.status(502).json({
        error:
          e.message
      });
    }
  }
);

/* =========================
   DASHBOARD
========================= */

app.get(
  '/api/dashboard',
  async (req, res) => {

    try {

      const d =
        await buildDashboard();

      lastDashboard =
        d;

      pushHistory(d);

      res.json(d);

    } catch (e) {

      res.status(502).json({

        ok: false,

        error:
          e.message,

        timestamp:
          new Date().toISOString()
      });
    }
  }
);

/* =========================
   SIGNAL HISTORY
========================= */

app.get(
  '/api/signal/history',
  (req, res) => {

    res.json({
      ok: true,
      items: history
    });
  }
);

/* =========================
   ALERTS
========================= */

app.get(
  '/api/alerts',
  (req, res) => {

    res.json({
      ok: true,
      items: alerts
    });
  }
);

/* =========================
   READ SIGNAL
========================= */

app.post(
  '/api/signal/history/:id/read',
  (req, res) => {

    const item =
      history.find(
        x =>
          x.id ===
          req.params.id
      );

    if (!item) {

      return res
        .status(404)
        .json({
          ok: false,
          error:
            'Signal not found'
        });
    }

    item.read = true;

    res.json({
      ok: true,
      item
    });
  }
);

/* =========================
   MARKET
========================= */

app.get(
  '/api/market',
  async (req, res) => {

    try {

      const [
        price,
        rows
      ] =
        await Promise.all([
          getSpot24h(),
          getSpotKlines(
            '5m',
            120
          )
        ]);

      res.json({

        ok: true,

        source:
          'Binance',

        price,

        analysis:
          timeframeAnalysis(
            candlesFromBinance(
              rows
            )
          )
      });

    } catch (e) {

      res.status(502).json({

        ok: false,

        error:
          e.message
      });
    }
  }
);

/* =========================
   WEBSOCKET
========================= */

wss.on(
  'connection',
  ws => {

    ws.send(
      JSON.stringify({

        type: 'hello',

        service:
          'BTC/USD AI SCALPING ENGINE V5',

        timestamp:
          new Date().toISOString()
      })
    );

    if (lastDashboard) {

      ws.send(
        JSON.stringify({
          type: 'dashboard',
          data:
            lastDashboard
        })
      );
    }
  }
);

/*
  IMPORTANT:
  Express 5 compatible SPA fallback.
  Do NOT use app.get('*').
*/

app.use(
  (req, res, next) => {

    if (req.method !== 'GET') {
      return next();
    }

    res.sendFile(
      path.join(
        __dirname,
        'frontend',
        'index.html'
      )
    );
  }
);

/* =========================
   START SERVER
========================= */

server.listen(
  PORT,
  HOST,
  () => {

    console.log(
      `BTC AI SCALPING ENGINE V5 listening on ${HOST}:${PORT}`
    );

    refresh();

    setInterval(
      refresh,
      15000
    );
  }
);
