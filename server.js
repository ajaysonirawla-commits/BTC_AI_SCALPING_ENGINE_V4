require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const WebSocket = require('ws');

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'frontend')));

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

const SPOT_BASES = [
  'https://data-api.binance.vision',
  'https://api.binance.com'
];

const BYBIT = 'https://api.bybit.com';
const BYBIT_WS = 'wss://stream.bybit.com/v5/public/linear';

const cache = {
  etf: { t: 0, data: null },
  macro: { t: 0, data: null },
  news: { t: 0, data: [] },
  flow: { t: 0, data: null }
};

const CACHE = {
  etf: 15 * 60e3,
  macro: 60e3,
  news: 2 * 60e3,
  flow: 8e3
};

const oiSnapshots = [];
const liqEvents = [];
const largeTrades = [];
const signalHistory = [];

let wsState = {
  connected: false,
  lastMessage: 0,
  reconnects: 0,
  provider: 'Bybit',
  lastError: null
};

let latestMarket = null;
let lastSignalId = 0;

/* =========================================================
   BASIC HELPERS
========================================================= */

function clamp(x, min = 0, max = 100) {
  return Math.max(min, Math.min(max, x));
}

function round(x, n = 2) {
  if (!Number.isFinite(x)) return null;

  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function avg(a) {
  return a.length
    ? a.reduce((x, y) => x + y, 0) / a.length
    : null;
}

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function getJson(base, url, opts = {}) {
  const fullUrl =
    /^https?:\/\//i.test(url)
      ? url
      : `${base}${url}`;

  const r = await fetch(fullUrl, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'BTC-AI-Scalping-Engine-V5'
    },
    ...opts
  });

  const txt = await r.text();

  let j;

  try {
    j = JSON.parse(txt);
  } catch {
    throw new Error(`Invalid JSON HTTP ${r.status}`);
  }

  if (!r.ok) {
    throw new Error(`HTTP ${r.status}`);
  }

  return j;
}

async function getJsonAny(bases, url, opts = {}) {
  let lastError = null;

  for (const base of bases) {
    try {
      return await getJson(base, url, opts);
    } catch (e) {
      lastError = e;
    }
  }

  throw lastError || new Error('No data source available');
}

async function getText(url) {
  const r = await fetch(url, {
    headers: {
      Accept: 'text/html,application/xml,text/xml',
      'User-Agent': 'BTC-AI-Scalping-Engine-V5'
    }
  });

  const text = await r.text();

  if (!r.ok) {
    throw new Error(`HTTP ${r.status}`);
  }

  return text;
}

/* =========================================================
   TECHNICAL INDICATORS
========================================================= */

function ema(a, n) {
  if (!a || a.length < n) return null;

  let e = avg(a.slice(0, n));
  const k = 2 / (n + 1);

  for (let i = n; i < a.length; i++) {
    e = a[i] * k + e * (1 - k);
  }

  return e;
}

function rsi(a, n = 14) {
  if (!a || a.length < n + 1) return null;

  let gain = 0;
  let loss = 0;

  for (let i = 1; i <= n; i++) {
    const d = a[i] - a[i - 1];

    gain += Math.max(d, 0);
    loss += Math.max(-d, 0);
  }

  let avgGain = gain / n;
  let avgLoss = loss / n;

  for (let i = n + 1; i < a.length; i++) {
    const d = a[i] - a[i - 1];

    avgGain =
      (avgGain * (n - 1) + Math.max(d, 0)) / n;

    avgLoss =
      (avgLoss * (n - 1) + Math.max(-d, 0)) / n;
  }

  if (avgLoss === 0) return 100;

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

function vwap(ks) {
  let pv = 0;
  let volume = 0;

  for (const k of ks) {
    const high = +k[2];
    const low = +k[3];
    const close = +k[4];
    const vol = +k[5];

    const typical =
      (high + low + close) / 3;

    pv += typical * vol;
    volume += vol;
  }

  return volume
    ? pv / volume
    : null;
}

function atr(ks, n = 14) {
  if (!ks || ks.length < n + 1) return null;

  const tr = [];

  for (let i = 1; i < ks.length; i++) {
    const high = +ks[i][2];
    const low = +ks[i][3];
    const prevClose = +ks[i - 1][4];

    tr.push(
      Math.max(
        high - low,
        Math.abs(high - prevClose),
        Math.abs(low - prevClose)
      )
    );
  }

  return avg(tr.slice(-n));
}

/* =========================================================
   BINANCE / BYBIT MARKET DATA
========================================================= */

function bybitKlineToBinance(k) {
  return [
    Number(k[0]),
    +k[1],
    +k[2],
    +k[3],
    +k[4],
    +k[5],
    0,
    +k[6],
    0,
    0,
    0,
    0
  ];
}

async function klines(interval, limit = 180) {
  try {
    return await getJsonAny(
      SPOT_BASES,
      `/api/v3/klines?symbol=BTCUSDT&interval=${interval}&limit=${limit}`
    );
  } catch {
    const intervals = {
      '1m': '1',
      '3m': '3',
      '5m': '5',
      '15m': '15',
      '30m': '30',
      '1h': '60',
      '2h': '120',
      '4h': '240',
      '1d': 'D'
    };

    const bybitInterval =
      intervals[interval] || '5';

    const j = await getJson(
      BYBIT,
      `/v5/market/kline?category=linear&symbol=BTCUSDT&interval=${bybitInterval}&limit=${Math.min(limit, 1000)}`
    );

    return (j.result?.list || [])
      .reverse()
      .map(bybitKlineToBinance);
  }
}

async function ticker() {
  const j = await getJson(
    BYBIT,
    '/v5/market/tickers?category=linear&symbol=BTCUSDT'
  );

  const x = j.result?.list?.[0];

  if (!x) {
    throw new Error('BTCUSDT ticker unavailable');
  }

  return {
    symbol: 'BTCUSDT',
    lastPrice: +(
      x.lastPrice ||
      x.markPrice ||
      0
    ),
    markPrice: +(x.markPrice || x.lastPrice || 0),
    indexPrice: +(x.indexPrice || 0),
    fundingRate: +(x.fundingRate || 0),
    nextFundingTime: +(x.nextFundingTime || 0),
    volume24h: +(x.volume24h || 0),
    turnover24h: +(x.turnover24h || 0),
    price24hPcnt: +(x.price24hPcnt || 0) * 100
  };
}

/* =========================================================
   TECHNICAL ANALYSIS
========================================================= */

function technical(ks) {
  const closes = ks.map(k => +k[4]);
  const volumes = ks.map(k => +k[5]);

  const price = closes.at(-1);

  const ema20 = ema(closes, 20);
  const ema50 = ema(closes, 50);

  const rv = rsi(closes, 14);
  const vw = vwap(ks);
  const a = atr(ks, 14);

  const volumeAverage =
    avg(volumes.slice(-20));

  const volumeRatio =
    volumeAverage
      ? volumes.at(-1) / volumeAverage
      : null;

  const high20 =
    Math.max(
      ...ks.slice(-20).map(k => +k[2])
    );

  const low20 =
    Math.min(
      ...ks.slice(-20).map(k => +k[3])
    );

  let trend = 'MIXED';

  if (
    price > ema20 &&
    ema20 > ema50
  ) {
    trend = 'BULLISH';
  } else if (
    price < ema20 &&
    ema20 < ema50
  ) {
    trend = 'BEARISH';
  }

  let score = 50;

  if (price > ema20) score += 8;
  else score -= 8;

  if (ema20 > ema50) score += 8;
  else score -= 8;

  if (price > vw) score += 8;
  else score -= 8;

  if (rv >= 50 && rv <= 70) {
    score += 8;
  } else if (rv >= 30 && rv < 50) {
    score -= 2;
  } else if (rv < 30) {
    score -= 8;
  } else if (rv > 70) {
    score += 2;
  }

  if (volumeRatio !== null) {
    if (volumeRatio > 1.5) {
      score += trend === 'BULLISH'
        ? 8
        : trend === 'BEARISH'
          ? -8
          : 0;
    } else if (volumeRatio > 1.2) {
      score += trend === 'BULLISH'
        ? 4
        : trend === 'BEARISH'
          ? -4
          : 0;
    }
  }

  return {
    price: round(price, 2),
    rsi: round(rv, 2),
    vwap: round(vw, 2),
    ema20: round(ema20, 2),
    ema50: round(ema50, 2),
    volumeRatio: round(volumeRatio, 3),
    trend,
    vwapBias:
      price >= vw
        ? 'ABOVE VWAP'
        : 'BELOW VWAP',
    atr: round(a, 2),
    high20: round(high20, 2),
    low20: round(low20, 2),
    score: clamp(Math.round(score))
  };
}

/* =========================================================
   OPEN INTEREST
========================================================= */

async function currentOI() {
  const j = await getJson(
    BYBIT,
    '/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=1'
  );

  const tick = await ticker();

  const row =
    j.result?.list?.[0];

  if (!row) {
    throw new Error('OI unavailable');
  }

  const btc =
    +(
      row.singleOpenInterest ||
      row.openInterest ||
      0
    );

  const usd =
    +(
      row.singleOpenInterestValue ||
      row.openInterestValue ||
      btc * tick.markPrice
    );

  return {
    btc: round(btc, 4),
    usd: round(usd, 2),
    markPrice: tick.markPrice,
    source: 'Bybit'
  };
}

function snapshotOI(v) {
  if (!Number.isFinite(v)) return;

  oiSnapshots.push({
    t: Date.now(),
    v
  });

  prune();
}

function oiChange(ms) {
  const now = Date.now();
  const last =
    oiSnapshots.at(-1);

  if (!last) return null;

  for (
    let i = oiSnapshots.length - 1;
    i >= 0;
    i--
  ) {
    if (
      now - oiSnapshots[i].t >= ms
    ) {
      if (!oiSnapshots[i].v) {
        return null;
      }

      return (
        (last.v / oiSnapshots[i].v - 1) *
        100
      );
    }
  }

  return null;
}

/* =========================================================
   FUNDING
========================================================= */

async function funding() {
  const t = await ticker();

  return {
    rate: round(t.fundingRate, 6),
    ratePct: round(
      t.fundingRate * 100,
      4
    ),
    markPrice: t.markPrice,
    nextFundingTime:
      t.nextFundingTime,
    source: 'Bybit'
  };
}

/* =========================================================
   LONG / SHORT ACCOUNT RATIO
========================================================= */

async function longShort() {
  try {
    const j = await getJson(
      BYBIT,
      '/v5/market/account-ratio?category=linear&symbol=BTCUSDT&period=5min&limit=1'
    );

    const x =
      j.result?.list?.[0];

    if (!x) return null;

    const buyRatio =
      +(
        x.buyRatio ||
        x.longAccount ||
        0
      );

    const sellRatio =
      +(
        x.sellRatio ||
        x.shortAccount ||
        0
      );

    return {
      long: round(buyRatio, 4),
      short: round(sellRatio, 4),
      ratio:
        sellRatio
          ? round(
              buyRatio / sellRatio,
              4
            )
          : null,
      source: 'Bybit'
    };
  } catch {
    return null;
  }
}

/* =========================================================
   LIQUIDATION + LARGE TRADE DATA
========================================================= */

function addLiq(side, usd) {
  if (
    !Number.isFinite(usd) ||
    usd <= 0
  ) {
    return;
  }

  liqEvents.push({
    t: Date.now(),
    side,
    usd
  });

  prune();
}

function addLarge(side, usd) {
  if (
    !Number.isFinite(usd) ||
    usd < 100000
  ) {
    return;
  }

  largeTrades.push({
    t: Date.now(),
    side,
    usd
  });

  prune();
}

function liqSummary() {
  prune();

  let longLiq = 0;
  let shortLiq = 0;

  for (const x of liqEvents) {
    if (x.side === 'LONG') {
      longLiq += x.usd;
    } else {
      shortLiq += x.usd;
    }
  }

  const total =
    longLiq + shortLiq;

  return {
    total1h: round(total, 2),
    long1h: round(longLiq, 2),
    short1h: round(shortLiq, 2),
    events: liqEvents.length,
    bias:
      total === 0
        ? 'NO RECENT LIQUIDATION'
        : shortLiq > longLiq
          ? 'SHORT LIQUIDATION HEAVY'
          : 'LONG LIQUIDATION HEAVY'
  };
}

function largeSummary() {
  prune();

  let buys = 0;
  let sells = 0;

  for (const x of largeTrades) {
    if (x.side === 'BUY') {
      buys += x.usd;
    } else {
      sells += x.usd;
    }
  }

  return {
    count: largeTrades.length,
    buyUsd: round(buys, 2),
    sellUsd: round(sells, 2),
    netUsd: round(
      buys - sells,
      2
    ),
    bias:
      buys === 0 && sells === 0
        ? 'NO LARGE-TRADE DATA'
        : buys > sells
          ? 'BUY PRESSURE'
          : 'SELL PRESSURE',
    source: 'Bybit public trade stream'
  };
}

/* =========================================================
   CLEAN OLD DATA
========================================================= */

function prune() {
  const now = Date.now();

  while (
    oiSnapshots[0] &&
    now - oiSnapshots[0].t >
      25 * 3600e3
  ) {
    oiSnapshots.shift();
  }

  while (
    liqEvents[0] &&
    now - liqEvents[0].t >
      3600e3
  ) {
    liqEvents.shift();
  }

  while (
    largeTrades[0] &&
    now - largeTrades[0].t >
      15 * 60e3
  ) {
    largeTrades.shift();
  }

  while (
    signalHistory.length > 100
  ) {
    signalHistory.shift();
  }
}

/* =========================================================
   FEAR & GREED
========================================================= */

async function fearGreed() {
  try {
    const j =
      await getJson(
        'https://api.alternative.me',
        '/fng/?limit=1'
      );

    return j.data?.[0] || null;
  } catch {
    return null;
  }
}

/* =========================================================
   ETF DATA
========================================================= */

function stripHtml(s) {
  return String(s || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseNumber(s) {
  s = stripHtml(s);

  if (
    !s ||
    s === '-' ||
    s === '—'
  ) {
    return null;
  }

  const negative =
    /^\(.*\)$/.test(s);

  const n =
    Number(
      s.replace(/[(),$]/g, '')
    );

  if (!Number.isFinite(n)) {
    return null;
  }

  return negative
    ? -n
    : n;
}

async function getETF() {
  if (
    cache.etf.data &&
    Date.now() - cache.etf.t <
      CACHE.etf
  ) {
    return cache.etf.data;
  }

  try {
    const html =
      await getText(
        'https://farside.co.uk/btc/'
      );

    const rows = [
      ...html.matchAll(
        /<tr[^>]*>([\s\S]*?)<\/tr>/gi
      )
    ];

    const result = [];

    for (const row of rows) {
      const cells = [
        ...row[1].matchAll(
          /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi
        )
      ].map(
        x => stripHtml(x[1])
      );

      if (
        cells.length >= 3 &&
        /^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$/
          .test(cells[0])
      ) {
        const values =
          cells
            .slice(1)
            .map(parseNumber);

        const total =
          values.at(-1);

        if (
          Number.isFinite(total)
        ) {
          result.push({
            date: cells[0],
            total
          });
        }
      }
    }

    const latest =
      result.at(-1);

    if (!latest) {
      throw new Error(
        'ETF table unavailable'
      );
    }

    cache.etf = {
      t: Date.now(),
      data: {
        ...latest,
        source: 'Farside'
      }
    };

    return cache.etf.data;

  } catch {
    return (
      cache.etf.data || null
    );
  }
}

/* =========================================================
   MACRO DATA
========================================================= */

async function yahooSeries(symbol) {
  const url =
    `https://query1.finance.yahoo.com/v8/finance/chart/` +
    `${encodeURIComponent(symbol)}` +
    `?range=1d&interval=5m`;

  const j =
    await getJson('', url);

  const r =
    j?.chart?.result?.[0];

  const q =
    r?.indicators?.quote?.[0];

  const closes =
    (q?.close || [])
      .filter(Number.isFinite);

  const last =
    closes.at(-1);

  const old =
    closes.length >= 7
      ? closes.at(-7)
      : null;

  return {
    last,
    old,
    change:
      old && last
        ? (
            (last / old - 1) *
            100
          )
        : null
  };
}

async function getMarkets() {
  const symbols = {
    DXY: 'DX-Y.NYB',
    NASDAQ: '^IXIC',
    SP500: '^GSPC',
    DOW: '^DJI',
    VIX: '^VIX',
    NIFTY: '^NSEI',
    SENSEX: '^BSESN',
    NIKKEI: '^N225'
  };

  try {
    const out = {};

    await Promise.all(
      Object.entries(symbols)
        .map(
          async ([name, symbol]) => {
            try {
              out[name] =
                await yahooSeries(symbol);
            } catch {
              out[name] = null;
            }
          }
        )
    );

    cache.macro = {
      t: Date.now(),
      data: out
    };

    return out;

  } catch {
    return (
      cache.macro.data || null
    );
  }
}

/* =========================================================
   NEWS
========================================================= */

async function getNews() {
  if (
    cache.news.data.length &&
    Date.now() - cache.news.t <
      CACHE.news
  ) {
    return cache.news.data;
  }

  const feeds = [
    [
      'CoinDesk',
      'https://www.coindesk.com/arc/outboundfeeds/rss/'
    ],
    [
      'Cointelegraph',
      'https://cointelegraph.com/rss'
    ]
  ];

  const all = [];

  for (const [
    source,
    url
  ] of feeds) {
    try {
      const xml =
        await getText(url);

      const items = [
        ...xml.matchAll(
          /<item>([\s\S]*?)<\/item>/gi
        )
      ];

      for (
        const item of items.slice(0, 8)
      ) {
        const block =
          item[1];

        const titleMatch =
          block.match(
            /<title[^>]*>([\s\S]*?)<\/title>/i
          );

        const linkMatch =
          block.match(
            /<link[^>]*>([\s\S]*?)<\/link>/i
          );

        const title =
          stripHtml(
            titleMatch?.[1]
          );

        const link =
          stripHtml(
            linkMatch?.[1]
          );

        if (title) {
          all.push({
            source,
            title,
            link
          });
        }
      }

    } catch {}
  }

  cache.news = {
    t: Date.now(),
    data: all.slice(0, 16)
  };

  return cache.news.data;
}

/* =========================================================
   VOLUME / FLOW
========================================================= */

async function getFlowStats() {
  if (
    cache.flow.data &&
    Date.now() - cache.flow.t <
      CACHE.flow
  ) {
    return cache.flow.data;
  }

  const map = {
    '1m': '1m',
    '5m': '5m',
    '10m': '15m',
    '30m': '30m',
    '1H': '1h',
    '1D': '1d'
  };

  const output = {};

  await Promise.all(
    Object.entries(map)
      .map(
        async ([tf, interval]) => {
          try {
            const k =
              await klines(
                interval,
                5
              );

            const last =
              k.at(-1);

            const previous =
              k.at(-2);

            const volume =
              +last[5];

            const prevVolume =
              +previous[5];

            const priceChange =
              (
                +last[4] /
                +last[1] -
                1
              ) * 100;

            output[tf] = {
              volume: round(
                volume,
                2
              ),
              volumeChange:
                prevVolume
                  ? round(
                      (
                        volume /
                        prevVolume -
                        1
                      ) * 100,
                      2
                    )
                  : null,
              priceChange:
                round(
                  priceChange,
                  3
                )
            };

          } catch {
            output[tf] = {
              volume: null,
              volumeChange: null,
              priceChange: null
            };
          }
        }
      )
  );

  cache.flow = {
    t: Date.now(),
    data: output
  };

  return output;
}

/* =========================================================
   MARKET SESSION
========================================================= */

function sessionStatus() {
  const now =
    new Date();

  const parts =
    new Intl.DateTimeFormat(
      'en-US',
      {
        timeZone: 'Asia/Kolkata',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
      }
    ).formatToParts(now);

  const hour =
    +parts.find(
      x => x.type === 'hour'
    ).value;

  const minute =
    +parts.find(
      x => x.type === 'minute'
    ).value;

  const mins =
    hour * 60 + minute;

  return {
    India:
      mins >= 555 &&
      mins < 930
        ? 'OPEN'
        : 'CLOSED',

    UK:
      mins >= 810 &&
      mins < 1320
        ? 'OPEN'
        : 'CLOSED',

    USA:
      mins >= 1140 ||
      mins < 90
        ? 'OPEN'
        : 'CLOSED',

    China:
      mins >= 420 &&
      mins < 810
        ? 'OPEN'
        : 'CLOSED',

    note:
      'Regular-session clock; holidays may differ'
  };
}

/* =========================================================
   SIGNAL COMPONENTS
========================================================= */

function technicalComponent(t) {
  if (!t) {
    return {
      score: 50,
      label: 'NO DATA'
    };
  }

  return {
    score: clamp(t.score),
    label: t.trend
  };
}

function oiComponent(change) {
  if (
    change === null ||
    !Number.isFinite(change)
  ) {
    return {
      score: 50,
      label: 'NO OI CHANGE DATA'
    };
  }

  return {
    score: clamp(
      50 + change * 8
    ),
    label:
      change > 0.5
        ? 'OI RISING'
        : change < -0.5
          ? 'OI FALLING'
          : 'OI FLAT'
  };
}

function fundingComponent(f) {
  if (
    !f ||
    !Number.isFinite(f.rate)
  ) {
    return {
      score: 50,
      label: 'NO FUNDING DATA'
    };
  }

  const rate =
    f.rate * 100;

  let score = 50;

  if (rate > 0.05) {
    score -= 15;
  } else if (rate > 0.02) {
    score -= 7;
  } else if (rate < -0.05) {
    score += 15;
  } else if (rate < -0.02) {
    score += 7;
  }

  return {
    score: clamp(score),
    label:
      rate > 0
        ? 'POSITIVE FUNDING'
        : 'NEGATIVE FUNDING'
  };
}

function liquidationComponent(liq) {
  if (
    !liq ||
    !liq.total1h
  ) {
    return {
      score: 50,
      label: 'NO LIQ DATA'
    };
  }

  let score = 50;

  if (
    liq.short1h >
    liq.long1h * 1.5
  ) {
    score = 68;
  } else if (
    liq.long1h >
    liq.short1h * 1.5
  ) {
    score = 32;
  }

  return {
    score,
    label: liq.bias
  };
}

function largeTradeComponent(large) {
  if (
    !large ||
    !large.count
  ) {
    return {
      score: 50,
      label: 'NO LARGE TRADE DATA'
    };
  }

  let score = 50;

  if (
    large.netUsd > 500000
  ) {
    score = 70;
  } else if (
    large.netUsd < -500000
  ) {
    score = 30;
  } else if (
    large.netUsd > 100000
  ) {
    score = 60;
  } else if (
    large.netUsd < -100000
  ) {
    score = 40;
  }

  return {
    score,
    label: large.bias
  };
}

function etfComponent(etf) {
  if (
    !etf ||
    !Number.isFinite(
      etf.total
    )
  ) {
    return {
      score: 50,
      label: 'ETF DATA UNAVAILABLE'
    };
  }

  return {
    score:
      etf.total > 0
        ? 65
        : etf.total < 0
          ? 35
          : 50,

    label:
      etf.total > 0
        ? 'ETF INFLOW'
        : etf.total < 0
          ? 'ETF OUTFLOW'
          : 'ETF FLAT'
  };
}

function macroComponent(markets) {
  if (!markets) {
    return {
      score: 50,
      label: 'MACRO DATA UNAVAILABLE'
    };
  }

  let score = 50;

  const dxy =
    markets.DXY?.change;

  const nasdaq =
    markets.NASDAQ?.change;

  const vix =
    markets.VIX?.change;

  if (
    Number.isFinite(dxy)
  ) {
    if (dxy < -0.2) score += 8;
    if (dxy > 0.2) score -= 8;
  }

  if (
    Number.isFinite(nasdaq)
  ) {
    if (nasdaq > 0.3) score += 7;
    if (nasdaq < -0.3) score -= 7;
  }

  if (
    Number.isFinite(vix)
  ) {
    if (vix > 5) score -= 5;
    if (vix < -5) score += 5;
  }

  return {
    score: clamp(score),
    label:
      score >= 58
        ? 'RISK-ON'
        : score <= 42
          ? 'RISK-OFF'
          : 'MIXED'
  };
}

function fearGreedComponent(fg) {
  if (!fg) {
    return {
      score: 50,
      label: 'NO FEAR/GREED DATA'
    };
  }

  const value =
    +fg.value;

  let score =
    clamp(value);

  /*
     Extreme fear can create rebound conditions.
     Extreme greed can create correction risk.
  */

  if (value >= 80) {
    score = 40;
  }

  if (value <= 20) {
    score = 60;
  }

  return {
    score,
    label:
      fg.value_classification ||
      'UNKNOWN'
  };
}

/* =========================================================
   WEIGHTED SIGNAL ENGINE
========================================================= */

function buildSignal({
  technicalData,
  oi,
  fund,
  liq,
  large,
  etf,
  macro,
  fg
}) {
  const components = {
    priceStructure: technicalComponent(
      technicalData
    ),

    rsi: {
      score:
        technicalData?.rsi === null
          ? 50
          : technicalData.rsi >= 50
            ? 60
            : 40,

      label:
        technicalData?.rsi === null
          ? 'NO RSI'
          : `RSI ${technicalData.rsi}`
    },

    emaVwap: {
      score:
        technicalData?.vwapBias === 'ABOVE VWAP'
          ? 65
          : 35,

      label:
        technicalData?.vwapBias ||
        'NO VWAP'
    },

    volume: {
      score:
        technicalData?.volumeRatio == null
          ? 50
          : technicalData.volumeRatio >= 1.3
            ? (
                technicalData.trend ===
                'BULLISH'
                  ? 70
                  : technicalData.trend ===
                    'BEARISH'
                    ? 30
                    : 50
              )
            : 50,

      label:
        technicalData?.volumeRatio == null
          ? 'NO VOLUME'
          : `VOL x${technicalData.volumeRatio}`
    },

    openInterest:
      oiComponent(
        oi
      ),

    funding:
      fundingComponent(
        fund
      ),

    liquidation:
      liquidationComponent(
        liq
      ),

    largeTrades:
      largeTradeComponent(
        large
      ),

    etf:
      etfComponent(
        etf
      ),

    macro:
      macroComponent(
        macro
      ),

    fearGreed:
      fearGreedComponent(
        fg
      )
  };

  const weights = {
    priceStructure: 15,
    rsi: 10,
    emaVwap: 10,
    volume: 10,
    openInterest: 15,
    funding: 5,
    liquidation: 10,
    largeTrades: 10,
    etf: 5,
    macro: 5,
    fearGreed: 5
  };

  let total = 0;

  for (
    const [name, weight]
    of Object.entries(weights)
  ) {
    total +=
      components[name].score *
      weight /
      100;
  }

  const score =
    clamp(
      Math.round(total)
    );

  let action = 'WAIT';

  if (score >= 70) {
    action = 'LONG';
  } else if (score <= 30) {
    action = 'SHORT';
  }

  const direction =
    action === 'LONG'
      ? 1
      : action === 'SHORT'
        ? -1
        : 0;

  const entry =
    technicalData?.price ||
    null;

  const atrValue =
    technicalData?.atr ||
    (
      entry
        ? entry * 0.003
        : null
    );

  let stopLoss = null;
  let target1 = null;
  let target2 = null;

  if (
    entry &&
    atrValue &&
    direction !== 0
  ) {
    const risk =
      Math.max(
        atrValue * 1.2,
        entry * 0.0015
      );

    stopLoss =
      entry -
      direction * risk;

    target1 =
      entry +
      direction * risk * 1.5;

    target2 =
      entry +
      direction * risk * 2.5;
  }

  const reasons = [];

  for (
    const [name, c]
    of Object.entries(components)
  ) {
    if (c.score >= 62) {
      reasons.push(
        `${name}: bullish`
      );
    }

    if (c.score <= 38) {
      reasons.push(
        `${name}: bearish`
      );
    }
  }

  return {
    score,
    action,

    confidence:
      score >= 70 ||
      score <= 30
        ? Math.abs(
            score - 50
          ) * 2
        : Math.abs(
            score - 50
          ) * 2,

    entry:
      round(entry, 2),

    stopLoss:
      round(stopLoss, 2),

    target1:
      round(target1, 2),

    target2:
      round(target2, 2),

    holdingWindow:
      action === 'WAIT'
        ? 'Wait for confirmation'
        : '5m–30m scalp window',

    components,
    weights,
    reasons,

    disclaimer:
      'Decision-support only — guaranteed profit नहीं'
  };
}

/* =========================================================
   FULL DASHBOARD BUILD
========================================================= */

async function buildDashboard() {
  const [
    t5,
    t1,
    t15,
    oi,
    fund,
    ls,
    fg
  ] = await Promise.all([
    klines('5m', 180),
    klines('1m', 180),
    klines('15m', 180),
    currentOI(),
    funding(),
    longShort(),
    fearGreed()
  ]);

  const technicalData =
    technical(t5);

  snapshotOI(oi.btc);

  const [
    etf,
    macro,
    flow
  ] = await Promise.all([
    getETF(),
    getMarkets(),
    getFlowStats()
  ]);

  const liq =
    liqSummary();

  const large =
    largeSummary();

  const signal =
    buildSignal({
      technicalData,
      oi: {
        ...oi,
        change5m:
          oiChange(5 * 60e3),
        change30m:
          oiChange(30 * 60e3),
        change1h:
          oiChange(60 * 60e3)
      },
      fund,
      liq,
      large,
      etf,
      macro,
      fg
    });

  const dashboard = {
    ok: true,
    timestamp:
      new Date().toISOString(),

    source: {
      spot: 'Binance public / fallback Bybit',
      derivatives: 'Bybit public',
      macro: 'Yahoo Finance public',
      etf: 'Farside public',
      fearGreed:
        'Alternative.me public'
    },

    btc: {
      price:
        technicalData.price,

      change24h:
        (
          await ticker()
        ).price24hPcnt,

      technical:
        technicalData
    },

    timeframes: {
      '1m':
        technical(t1),

      '5m':
        technical(t5),

      '15m':
        technical(t15)
    },

    openInterest: {
      ...oi,
      change5m:
        oiChange(5 * 60e3),
      change30m:
        oiChange(30 * 60e3),
      change1h:
        oiChange(60 * 60e3)
    },

    funding: fund,

    longShort: ls,

    liquidation: liq,

    largeTrades: large,

    etf,

    fearGreed: fg,

    macro,

    flow,

    sessions:
      sessionStatus(),

    signal
  };

  latestMarket =
    dashboard;

  return dashboard;
}

/* =========================================================
   SIGNAL HISTORY
========================================================= */

function saveSignal(signal) {
  if (!signal) return;

  const item = {
    id: ++lastSignalId,
    timestamp:
      new Date().toISOString(),

    score: signal.score,
    action: signal.action,

    entry: signal.entry,
    stopLoss: signal.stopLoss,
    target1: signal.target1,
    target2: signal.target2,

    confidence:
      signal.confidence,

    read: false
  };

  signalHistory.push(item);

  prune();

  return item;
}

/* =========================================================
   WEBSOCKET
========================================================= */

let ws = null;
let wsReconnectTimer = null;

function connectBybitWS() {
  if (ws) {
    try {
      ws.close();
    } catch {}
  }

  wsState.connected = false;

  try {
    ws =
      new WebSocket(
        BYBIT_WS
      );

    ws.on('open', () => {
      wsState.connected = true;
      wsState.lastError = null;

      ws.send(
        JSON.stringify({
          op: 'subscribe',
          args: [
            'publicTrade.BTCUSDT',
            'allLiquidation.BTCUSDT'
          ]
        })
      );
    });

    ws.on('message', raw => {
      wsState.lastMessage =
        Date.now();

      try {
        const msg =
          JSON.parse(
            raw.toString()
          );

        const topic =
          msg.topic || '';

        const data =
          msg.data;

        if (
          topic.startsWith(
            'publicTrade.'
          ) &&
          Array.isArray(data)
        ) {
          for (const trade of data) {
            const price =
              +trade.p;

            const size =
              +trade.v;

            const usd =
              price * size;

            const side =
              String(
                trade.S ||
                trade.side ||
                ''
              ).toUpperCase();

            addLarge(
              side === 'BUY'
                ? 'BUY'
                : 'SELL',
              usd
            );
          }
        }

        if (
          topic.startsWith(
            'allLiquidation.'
          ) &&
          Array.isArray(data)
        ) {
          for (const x of data) {
            const price =
              +(
                x.price ||
                0
              );

            const size =
              +(
                x.size ||
                x.qty ||
                0
              );

            const usd =
              price * size;

            const side =
              String(
                x.side ||
                ''
              ).toUpperCase();

            /*
              A SELL liquidation generally
              represents a long liquidation.
              A BUY liquidation generally
              represents a short liquidation.
            */

            addLiq(
              side === 'SELL'
                ? 'LONG'
                : 'SHORT',
              usd
            );
          }
        }

      } catch {}
    });

    ws.on('close', () => {
      wsState.connected =
        false;

      scheduleReconnect();
    });

    ws.on('error', err => {
      wsState.lastError =
        String(
          err?.message ||
          err
        );
    });

  } catch (e) {
    wsState.lastError =
      String(
        e?.message ||
        e
      );

    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (wsReconnectTimer) {
    return;
  }

  wsState.reconnects++;

  wsReconnectTimer =
    setTimeout(() => {
      wsReconnectTimer = null;
      connectBybitWS();
    }, 5000);
}

/* =========================================================
   API ROUTES
========================================================= */

app.get(
  '/api/health',
  async (req, res) => {
    res.json({
      ok: true,
      service:
        'BTC/USD AI SCALPING ENGINE V5',
      time:
        new Date().toISOString(),

      websocket: wsState,

      uptime:
        process.uptime(),

      node:
        process.version
    });
  }
);

app.get(
  '/api/price',
  async (req, res) => {
    try {
      res.json(
        await ticker()
      );
    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/klines',
  async (req, res) => {
    try {
      const interval =
        req.query.interval ||
        '5m';

      const limit =
        Math.min(
          Number(
            req.query.limit ||
            180
          ),
          1000
        );

      res.json({
        ok: true,
        interval,
        data:
          await klines(
            interval,
            limit
          )
      });

    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/oi',
  async (req, res) => {
    try {
      const oi =
        await currentOI();

      snapshotOI(
        oi.btc
      );

      res.json({
        ok: true,
        ...oi,
        change5m:
          oiChange(
            5 * 60e3
          ),
        change30m:
          oiChange(
            30 * 60e3
          ),
        change1h:
          oiChange(
            60 * 60e3
          )
      });

    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/funding',
  async (req, res) => {
    try {
      res.json({
        ok: true,
        ...(await funding())
      });
    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/liquidations',
  (req, res) => {
    res.json({
      ok: true,
      ...liqSummary()
    });
  }
);

app.get(
  '/api/large-trades',
  (req, res) => {
    res.json({
      ok: true,
      ...largeSummary()
    });
  }
);

app.get(
  '/api/etf',
  async (req, res) => {
    res.json({
      ok: true,
      data:
        await getETF()
    });
  }
);

app.get(
  '/api/fear-greed',
  async (req, res) => {
    res.json({
      ok: true,
      data:
        await fearGreed()
    });
  }
);

app.get(
  '/api/macro',
  async (req, res) => {
    res.json({
      ok: true,
      data:
        await getMarkets()
    });
  }
);

app.get(
  '/api/news',
  async (req, res) => {
    res.json({
      ok: true,
      data:
        await getNews()
    });
  }
);

app.get(
  '/api/flow',
  async (req, res) => {
    res.json({
      ok: true,
      data:
        await getFlowStats()
    });
  }
);

app.get(
  '/api/decision',
  async (req, res) => {
    try {
      const d =
        latestMarket ||
        await buildDashboard();

      res.json({
        ok: true,
        signal:
          d.signal,
        timestamp:
          d.timestamp
      });

    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/dashboard',
  async (req, res) => {
    try {
      const d =
        await buildDashboard();

      const previous =
        signalHistory.at(-1);

      const signalChanged =
        !previous ||
        previous.action !==
          d.signal.action ||
        previous.score !==
          d.signal.score;

      if (signalChanged) {
        saveSignal(
          d.signal
        );
      }

      res.json(d);

    } catch (e) {
      console.error(
        'Dashboard error:',
        e
      );

      res.status(503).json({
        ok: false,
        error:
          e.message,
        timestamp:
          new Date().toISOString()
      });
    }
  }
);

/* =========================================================
   ALIASES FOR FRONTEND COMPATIBILITY
========================================================= */

app.get(
  '/api/summary',
  async (req, res) => {
    try {
      const d =
        await buildDashboard();

      res.json({
        ok: true,
        btc: d.btc,
        signal: d.signal,
        openInterest:
          d.openInterest,
        funding:
          d.funding,
        liquidation:
          d.liquidation,
        largeTrades:
          d.largeTrades
      });

    } catch (e) {
      res.status(503).json({
        ok: false,
        error: e.message
      });
    }
  }
);

app.get(
  '/api/signals',
  (req, res) => {
    res.json({
      ok: true,
      count:
        signalHistory.length,
      data:
        signalHistory
          .slice()
          .reverse()
    });
  }
);

app.get(
  '/api/history',
  (req, res) => {
    res.json({
      ok: true,
      count:
        signalHistory.length,
      data:
        signalHistory
          .slice()
          .reverse()
    });
  }
);

app.post(
  '/api/signals/:id/read',
  (req, res) => {
    const id =
      Number(
        req.params.id
      );

    const item =
      signalHistory.find(
        x => x.id === id
      );

    if (!item) {
      return res.status(404)
        .json({
          ok: false,
          error:
            'Signal not found'
        });
    }

    item.read = true;

    res.json({
      ok: true,
      data: item
    });
  }
);

app.post(
  '/api/signals/read-all',
  (req, res) => {
    for (
      const x of signalHistory
    ) {
      x.read = true;
    }

    res.json({
      ok: true
    });
  }
);

/* =========================================================
   FRONTEND FALLBACK
========================================================= */

app.get(
  '*',
  (req, res) => {
    if (
      req.path.startsWith('/api/')
    ) {
      return res.status(404)
        .json({
          ok: false,
          error:
            'API endpoint not found'
        });
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

/* =========================================================
   ERROR HANDLER
========================================================= */

app.use(
  (err, req, res, next) => {
    console.error(
      'Server error:',
      err
    );

    res.status(500).json({
      ok: false,
      error:
        err.message ||
        'Internal server error'
    });
  }
);

/* =========================================================
   START SERVER
========================================================= */

app.listen(
  PORT,
  HOST,
  () => {
    console.log(
      `BTC AI Scalping Engine V5 running on ${HOST}:${PORT}`
    );

    console.log(
      `Health: /api/health`
    );

    console.log(
      `Dashboard: /api/dashboard`
    );

    connectBybitWS();
  }
);
