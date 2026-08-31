require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const WebSocket = require('ws');

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'frontend')));

const SPOT_BASES = [
  'https://data-api.binance.vision',
  'https://api.binance.com'
];

const BYBIT = 'https://api.bybit.com';

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

const cache = {
  etf: { t: 0, data: null },
  macro: { t: 0, data: null },
  news: { t: 0, data: [] },
  flow: { t: 0, data: null }
};

const CACHE = {
  etf: 15 * 60e3,
  macro: 30e3,
  news: 2 * 60e3,
  flow: 8e3
};

const oiSnapshots = [];
const liqEvents = [];
const largeTrades = [];

let wsState = {
  connected: false,
  lastMessage: 0,
  reconnects: 0,
  provider: 'Bybit'
};

async function getJson(base, url, opts = {}) {
  const r = await fetch((base || '') + url, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'BTC-Scalping-V5'
    },
    ...opts
  });

  const txt = await r.text();

  let j;

  try {
    j = JSON.parse(txt);
  } catch {
    throw Error(`Invalid JSON HTTP ${r.status}`);
  }

  if (!r.ok) {
    throw Error(`HTTP ${r.status}`);
  }

  return j;
}

async function getJsonAny(bases, url, opts = {}) {
  let last;

  for (const base of bases) {
    try {
      return await getJson(base, url, opts);
    } catch (e) {
      last = e;
    }
  }

  throw last || Error('No data source');
}

async function getText(url) {
  const r = await fetch(url, {
    headers: {
      Accept: 'text/html,application/xml,text/xml',
      'User-Agent': 'BTC-Scalping-V5'
    }
  });

  const t = await r.text();

  if (!r.ok) {
    throw Error(`HTTP ${r.status}`);
  }

  return t;
}

const avg = a =>
  a.length
    ? a.reduce((x, y) => x + y, 0) / a.length
    : null;

function ema(a, n) {
  if (a.length < n) return null;

  let e = avg(a.slice(0, n));
  const k = 2 / (n + 1);

  for (let i = n; i < a.length; i++) {
    e = a[i] * k + e * (1 - k);
  }

  return e;
}

function rsi(a, n = 14) {
  if (a.length < n + 1) return null;

  let g = 0;
  let l = 0;

  for (let i = 1; i <= n; i++) {
    const d = a[i] - a[i - 1];

    g += Math.max(d, 0);
    l += Math.max(-d, 0);
  }

  let ag = g / n;
  let al = l / n;

  for (let i = n + 1; i < a.length; i++) {
    const d = a[i] - a[i - 1];

    ag = (ag * (n - 1) + Math.max(d, 0)) / n;
    al = (al * (n - 1) + Math.max(-d, 0)) / n;
  }

  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

function vwap(ks) {
  let pv = 0;
  let v = 0;

  for (const k of ks) {
    const h = +k[2];
    const l = +k[3];
    const c = +k[4];
    const vol = +k[5];

    pv += ((h + l + c) / 3) * vol;
    v += vol;
  }

  return v ? pv / v : null;
}

function atr(ks, n = 14) {
  if (ks.length < n + 1) return null;

  const tr = [];

  for (let i = 1; i < ks.length; i++) {
    const h = +ks[i][2];
    const l = +ks[i][3];
    const pc = +ks[i - 1][4];

    tr.push(
      Math.max(
        h - l,
        Math.abs(h - pc),
        Math.abs(l - pc)
      )
    );
  }

  return avg(tr.slice(-n));
}

function clamp(x) {
  return Math.max(0, Math.min(100, x));
}

function technical(ks) {
  const c = ks.map(k => +k[4]);
  const vol = ks.map(k => +k[5]);

  const price = c.at(-1);
  const e20 = ema(c, 20);
  const e50 = ema(c, 50);
  const rv = rsi(c);
  const vw = vwap(ks);
  const a = atr(ks);

  const av = avg(vol.slice(-20));
  const vr = av ? vol.at(-1) / av : null;

  const trend =
    price > e20 && e20 > e50
      ? 'BULLISH'
      : price < e20 && e20 < e50
        ? 'BEARISH'
        : 'MIXED';

  let score = 50;

  if (price > e20) score += 10;
  else score -= 10;

  if (e20 > e50) score += 10;
  else score -= 10;

  if (price > vw) score += 10;
  else score -= 10;

  if (rv >= 50 && rv <= 70) {
    score += 10;
  } else if (rv < 40) {
    score -= 10;
  }

  if (vr > 1.2) {
    score += trend === 'BULLISH'
      ? 10
      : trend === 'BEARISH'
        ? -10
        : 0;
  }

  return {
    price,
    rsi: rv,
    vwap: vw,
    ema20: e20,
    ema50: e50,
    volumeRatio: vr,
    trend,
    vwapBias: price > vw ? 'ABOVE VWAP' : 'BELOW VWAP',
    atr: a,
    score: clamp(Math.round(score)),
    high: Math.max(...ks.slice(-20).map(k => +k[2])),
    low: Math.min(...ks.slice(-20).map(k => +k[3]))
  };
}

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
    const mins = {
      '1m': '1',
      '5m': '5',
      '10m': '15',
      '15m': '15',
      '30m': '30',
      '1h': '60',
      '1d': 'D'
    }[interval] || '5';

    const j = await getJson(
      BYBIT,
      `/v5/market/kline?category=linear&symbol=BTCUSDT&interval=${mins}&limit=${Math.min(limit, 1000)}`
    );

    return (j.result?.list || [])
      .reverse()
      .map(bybitKlineToBinance);
  }
}

async function tfTech(i) {
  return technical(await klines(i, 180));
}

async function currentOI() {
  const j = await getJson(
    BYBIT,
    '/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=1'
  );

  const t = await getJson(
    BYBIT,
    '/v5/market/tickers?category=linear&symbol=BTCUSDT'
  );

  const row = j.result?.list?.[0];
  const tick = t.result?.list?.[0];

  const btc = +(row?.openInterest || 0);
  const mark = +(tick?.markPrice || tick?.lastPrice || 0);

  return {
    btc,
    usd: btc * mark,
    markPrice: mark,
    source: 'Bybit'
  };
}

async function funding() {
  const t = await getJson(
    BYBIT,
    '/v5/market/tickers?category=linear&symbol=BTCUSDT'
  );

  const x = t.result?.list?.[0] || {};

  return {
    rate: +(x.fundingRate || 0),
    markPrice: +(x.markPrice || x.lastPrice || 0),
    nextFundingTime: +(x.nextFundingTime || 0),
    source: 'Bybit'
  };
}

async function longShort() {
  try {
    const j = await getJson(
      BYBIT,
      '/v5/market/account-ratio?category=linear&symbol=BTCUSDT&period=5min&limit=1'
    );

    const x = j.result?.list?.[0];

    if (!x) return null;

    const ratio = +(x.buyRatio || 0);

    return {
      long: ratio,
      short: 1 - ratio,
      ratio: ratio / (1 - ratio || 1)
    };
  } catch {
    return null;
  }
}

async function fearGreed() {
  try {
    return (
      await getJson(
        'https://api.alternative.me',
        '/fng/?limit=1'
      )
    ).data?.[0] || null;
  } catch {
    return null;
  }function prune() {
  const n = Date.now();

  while (
    oiSnapshots[0] &&
    n - oiSnapshots[0].t > 25 * 3600e3
  ) {
    oiSnapshots.shift();
  }

  while (
    liqEvents[0] &&
    n - liqEvents[0].t > 3600e3
  ) {
    liqEvents.shift();
  }

  while (
    largeTrades[0] &&
    n - largeTrades[0].t > 15 * 60e3
  ) {
    largeTrades.shift();
  }
}

function snapshotOI(v) {
  oiSnapshots.push({
    t: Date.now(),
    v
  });

  prune();
}

function changeSince(ms) {
  const n = Date.now();
  const last = oiSnapshots.at(-1);

  if (!last) return null;

  for (
    let i = oiSnapshots.length - 1;
    i >= 0;
    i--
  ) {
    if (n - oiSnapshots[i].t >= ms) {
      return oiSnapshots[i].v
        ? ((last.v / oiSnapshots[i].v) - 1) * 100
        : null;
    }
  }

  return null;
}

function addLiq(side, usd) {
  liqEvents.push({
    t: Date.now(),
    side,
    usd
  });

  prune();
}

function addLarge(side, usd) {
  if (usd >= 100000) {
    largeTrades.push({
      t: Date.now(),
      side,
      usd
    });
  }

  prune();
}

function liqSummary() {
  prune();

  let lo = 0;
  let sh = 0;

  for (const x of liqEvents) {
    if (x.side === 'LONG') {
      lo += x.usd;
    } else {
      sh += x.usd;
    }
  }

  return {
    total1h: lo + sh,
    long1h: lo,
    short1h: sh,
    bias:
      lo + sh === 0
        ? 'NO RECENT LIQUIDATION'
        : sh > lo
          ? 'SHORT LIQUIDATION HEAVY'
          : 'LONG LIQUIDATION HEAVY',
    events: liqEvents.length
  };
}

function largeSummary() {
  prune();

  let b = 0;
  let s = 0;

  for (const x of largeTrades) {
    if (x.side === 'BUY') {
      b += x.usd;
    } else {
      s += x.usd;
    }
  }

  return {
    count: largeTrades.length,
    buyUsd: b,
    sellUsd: s,
    netUsd: b - s,
    bias:
      largeTrades.length
        ? b > s
          ? 'BUY PRESSURE'
          : 'SELL PRESSURE'
        : 'NO LARGE-TRADE DATA',
    note:
      'Bybit aggregate-trade proxy; not wallet-level institutional flow'
  };
}

function strip(s) {
  return String(s || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function num(s) {
  s = strip(s);

  if (!s || s === '-' || s === '—') {
    return null;
  }

  const neg = /^\(.*\)$/.test(s);

  const n = Number(
    s.replace(/[(),$]/g, '')
  );

  return Number.isFinite(n)
    ? neg
      ? -n
      : n
    : null;
}

async function getETF() {
  if (
    cache.etf.data &&
    Date.now() - cache.etf.t < CACHE.etf
  ) {
    return cache.etf.data;
  }

  try {
    const html = await getText(
      'https://farside.co.uk/btc/'
    );

    const rows = [
      ...html.matchAll(
        /<tr[^>]*>([\s\S]*?)<\/tr>/gi
      )
    ];

    const a = [];

    for (const row of rows) {
      const c = [
        ...row[1].matchAll(
          /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi
        )
      ].map(x => strip(x[1]));

      if (
        c.length >= 3 &&
        /^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$/.test(c[0])
      ) {
        const vals = c
          .slice(1)
          .map(num);

        const total = vals.at(-1);

        if (Number.isFinite(total)) {
          a.push({
            date: c[0],
            total
          });
        }
      }
    }

    const x = a.at(-1);

    if (!x) {
      throw Error(
        'ETF table unavailable'
      );
    }

    cache.etf = {
      t: Date.now(),
      data: {
        ...x,
        source: 'Farside Investors'
      }
    };

    return cache.etf.data;

  } catch {
    return cache.etf.data || null;
  }
}

async function yahooSeries(symbol) {
  const u =
    `https://query1.finance.yahoo.com/v8/finance/chart/` +
    `${encodeURIComponent(symbol)}` +
    `?range=1d&interval=5m`;

  const j = await getJson('', u);

  const r =
    j?.chart?.result?.[0];

  const q =
    r?.indicators?.quote?.[0];

  const c =
    (q?.close || [])
      .filter(Number.isFinite);

  return {
    last: c.at(-1) ?? null,
    old: c.length >= 7
      ? c.at(-7)
      : null,
    change:
      c.length >= 7
        ? ((c.at(-1) / c.at(-7)) - 1) * 100
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
    FTSE: '^FTSE',
    NIKKEI: '^N225',
    SHANGHAI: '000001.SS',
    NIFTY: '^NSEI',
    SENSEX: '^BSESN'
  };

  try {
    const out = {};

    await Promise.all(
      Object.entries(symbols)
        .map(async ([k, s]) => {
          out[k] =
            await yahooSeries(s);
        })
    );

    cache.macro = {
      t: Date.now(),
      data: out
    };

    return out;

  } catch {
    return cache.macro.data || null;
  }
}

async function getNews() {
  if (
    cache.news.data.length &&
    Date.now() - cache.news.t < CACHE.news
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

  for (const [source, url] of feeds) {
    try {
      const xml =
        await getText(url);

      for (
        const m of [
          ...xml.matchAll(
            /<item>([\s\S]*?)<\/item>/gi
          )
        ].slice(0, 6)
      ) {
        const b = m[1];

        const title =
          strip(
            (
              b.match(
                /<title[^>]*>([\s\S]*?)<\/title>/i
              ) || []
            )[1]
          );

        const link =
          strip(
            (
              b.match(
                /<link[^>]*>([\s\S]*?)<\/link>/i
              ) || []
            )[1]
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
    data: all.slice(0, 12)
  };

  return cache.news.data;
}

async function getFlowStats() {
  if (
    cache.flow.data &&
    Date.now() - cache.flow.t < CACHE.flow
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

  const out = {};

  await Promise.all(
    Object.entries(map)
      .map(async ([tf, i]) => {
        try {
          const k =
            await klines(i, 3);

          const vols =
            k.map(x => +x[7]);

          const last =
            vols.at(-1);

          const prev =
            vols.at(-2);

          out[tf] = {
            volume: last,
            volumeChange:
              prev
                ? ((last / prev) - 1) * 100
                : null,
            priceChange:
              +k.at(-1)[4] -
              +k.at(-1)[1]
          };

        } catch {
          out[tf] = {
            volume: null,
            volumeChange: null,
            priceChange: null
          };
        }
      })
  );

  cache.flow = {
    t: Date.now(),
    data: out
  };

  return out;
}

function sessionStatus() {
  const now = new Date();

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

  const us =
    mins >= 1140 || mins < 90;

  const uk =
    mins >= 810 && mins < 1320;

  const india =
    mins >= 555 && mins < 930;

  const china =
    mins >= 420 && mins < 810;

  return {
    India: india
      ? 'OPEN'
      : 'CLOSED',

    USA: us
      ? 'OPEN'
      : 'CLOSED',

    UK: uk
      ? 'OPEN'
      : 'CLOSED',

    China: china
      ? 'OPEN'
      : 'CLOSED',

    note:
      'Regular-session clock; holidays may differ'
  };
}
}
