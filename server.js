require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const WebSocket = require('ws');

const app = express();

app.use(cors());
app.use(express.json());

app.use(express.static(
  path.join(__dirname, 'frontend')
));

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';

const SYMBOL = 'BTCUSDT';

const SPOT_BASES = [
  'https://api.binance.com',
  'https://data-api.binance.vision'
];

const FUTURES_BASES = [
  'https://fapi.binance.com'
];

/* =========================================================
   RUNTIME STATE
========================================================= */

const cache = new Map();
const previous = new Map();

let streamConnected = false;
let streamLastMessage = 0;
let streamReconnects = 0;

let ws = null;
let wsTimer = null;


/* =========================================================
   BASIC HELPERS
========================================================= */

function finite(x) {

  const n = Number(x);

  return Number.isFinite(n)
    ? n
    : null;
}


function clamp(n, min, max) {

  return Math.max(
    min,
    Math.min(max, n)
  );
}


function pctChange(current, old) {

  if (
    !Number.isFinite(current) ||
    !Number.isFinite(old) ||
    old === 0
  ) {
    return null;
  }

  return ((current - old) / old) * 100;
}


/* =========================================================
   FETCH JSON WITH TIMEOUT
========================================================= */

async function fetchJSON(
  url,
  options = {},
  timeoutMs = 12000
) {

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      timeoutMs
    );

  try {

    const response =
      await fetch(
        url,
        {
          ...options,

          signal:
            controller.signal,

          headers: {
            'Accept':
              'application/json',

            'User-Agent':
              'BTC-AI-SCALPING-ENGINE-V5/1.0',

            ...(options.headers || {})
          }
        }
      );

    const text =
      await response.text();

    let data;

    try {

      data =
        JSON.parse(text);

    } catch {

      throw new Error(
        `Non-JSON response HTTP ${response.status}`
      );

    }

    if (!response.ok) {

      throw new Error(
        data?.msg ||
        data?.message ||
        `HTTP ${response.status}`
      );

    }

    return data;

  } finally {

    clearTimeout(timer);

  }

}


/* =========================================================
   TRY MULTIPLE BINANCE ENDPOINTS
========================================================= */

async function firstWorking(
  bases,
  endpoint
) {

  let lastError = null;

  for (const base of bases) {

    try {

      const data =
        await fetchJSON(
          base + endpoint
        );

      return {
        data,
        base
      };

    } catch (error) {

      lastError = error;

    }

  }

  throw (
    lastError ||
    new Error(
      'No endpoint available'
    )
  );

}


/* =========================================================
   CACHE
========================================================= */

function cacheSet(
  key,
  value,
  ttlMs
) {

  cache.set(
    key,
    {
      value,
      expires:
        Date.now() + ttlMs
    }
  );

}


function cacheGet(key) {

  const item =
    cache.get(key);

  if (!item)
    return null;

  if (
    item.expires <
    Date.now()
  ) {

    cache.delete(key);

    return null;
  }

  return item.value;

}


/* =========================================================
   BINANCE SPOT 24H
========================================================= */

async function spot24h() {

  const cached =
    cacheGet('spot24h');

  if (cached)
    return cached;

  const response =
    await firstWorking(
      SPOT_BASES,
      `/api/v3/ticker/24hr?symbol=${SYMBOL}`
    );

  const data =
    response.data;

  const result = {

    price:
      finite(data.lastPrice),

    change:
      finite(data.priceChangePercent),

    volume24h:
      finite(data.volume),

    quoteVolume24h:
      finite(data.quoteVolume),

    source:
      'Binance Spot'

  };

  cacheSet(
    'spot24h',
    result,
    2500
  );

  return result;

}


/* =========================================================
   BINANCE CANDLES
========================================================= */

async function klines(
  interval,
  limit = 120
) {

  const key =
    `kline-${interval}`;

  const cached =
    cacheGet(key);

  if (cached)
    return cached;

  const response =
    await firstWorking(
      SPOT_BASES,

      `/api/v3/klines` +
      `?symbol=${SYMBOL}` +
      `&interval=${interval}` +
      `&limit=${limit}`
    );

  const rows =
    response.data.map(
      x => ({

        time:
          Number(x[0]),

        open:
          Number(x[1]),

        high:
          Number(x[2]),

        low:
          Number(x[3]),

        close:
          Number(x[4]),

        volume:
          Number(x[5])

      })
    );

  cacheSet(
    key,
    rows,
    4500
  );

  return rows;

}


/* =========================================================
   EMA
========================================================= */

function ema(
  values,
  period
) {

  if (!values.length)
    return null;

  const k =
    2 / (period + 1);

  let result =
    values[0];

  for (
    let i = 1;
    i < values.length;
    i++
  ) {

    result =
      values[i] * k +
      result * (1 - k);

  }

  return result;

}


/* =========================================================
   RSI
========================================================= */

function rsi(
  values,
  period = 14
) {

  if (
    values.length <= period
  ) {

    return null;

  }

  let gain = 0;
  let loss = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {

    const diff =
      values[i] -
      values[i - 1];

    if (diff >= 0)
      gain += diff;
    else
      loss -= diff;

  }

  let avgGain =
    gain / period;

  let avgLoss =
    loss / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {

    const diff =
      values[i] -
      values[i - 1];

    const currentGain =
      diff > 0
        ? diff
        : 0;

    const currentLoss =
      diff < 0
        ? -diff
        : 0;

    avgGain =
      (
        avgGain * (period - 1) +
        currentGain
      ) / period;

    avgLoss =
      (
        avgLoss * (period - 1) +
        currentLoss
      ) / period;

  }

  if (avgLoss === 0)
    return 100;

  const rs =
    avgGain / avgLoss;

  return (
    100 -
    (100 / (1 + rs))
  );

}


/* =========================================================
   ATR
========================================================= */

function atr(
  rows,
  period = 14
) {

  if (
    rows.length <= period
  ) {

    return null;

  }

  const tr = [];

  for (
    let i = 1;
    i < rows.length;
    i++
  ) {

    tr.push(

      Math.max(

        rows[i].high -
        rows[i].low,

        Math.abs(
          rows[i].high -
          rows[i - 1].close
        ),

        Math.abs(
          rows[i].low -
          rows[i - 1].close
        )

      )

    );

  }

  const recent =
    tr.slice(-period);

  return (
    recent.reduce(
      (a, b) => a + b,
      0
    ) /
    recent.length
  );

}


/* =========================================================
   VWAP
========================================================= */

function vwap(rows) {

  const recent =
    rows.slice(-60);

  let priceVolume = 0;
  let volume = 0;

  for (const row of recent) {

    const typicalPrice =
      (
        row.high +
        row.low +
        row.close
      ) / 3;

    priceVolume +=
      typicalPrice *
      row.volume;

    volume +=
      row.volume;

  }

  if (!volume)
    return null;

  return (
    priceVolume /
    volume
  );

}


/* =========================================================
   TREND
========================================================= */

function trend(rows) {

  if (rows.length < 55)
    return 'MIXED';

  const closes =
    rows.map(
      x => x.close
    );

  const ema20Value =
    ema(
      closes,
      20
    );

  const ema50Value =
    ema(
      closes,
      50
    );

  const last =
    closes.at(-1);

  if (
    last >
    ema20Value &&
    ema20Value >
    ema50Value
  ) {

    return 'BULLISH';

  }

  if (
    last <
    ema20Value &&
    ema20Value <
    ema50Value
  ) {

    return 'BEARISH';

  }

  return 'MIXED';

}


/* =========================================================
   TECHNICAL PACKAGE
========================================================= */

function technical(rows) {

  const closes =
    rows.map(
      x => x.close
    );

  return {

    rsi:
      rsi(
        closes,
        14
      ),

    ema20:
      ema(
        closes,
        20
      ),

    ema50:
      ema(
        closes,
        50
      ),

    vwap:
      vwap(rows),

    atr:
      atr(
        rows,
        14
      ),

    trend:
      trend(rows),

    high20:
      Math.max(
        ...rows
          .slice(-20)
          .map(x => x.high)
      ),

    low20:
      Math.min(
        ...rows
          .slice(-20)
          .map(x => x.low)
      ),

    last:
      closes.at(-1)

  };

}


/* =========================================================
   BINANCE FUTURES
========================================================= */

async function futuresStats() {

  const cached =
    cacheGet(
      'futuresStats'
    );

  if (cached)
    return cached;

  const [
    premiumResult,
    oiResult,
    fundingResult,
    longShortResult
  ] =
    await Promise.allSettled([

      firstWorking(
        FUTURES_BASES,

        `/fapi/v1/premiumIndex` +
        `?symbol=${SYMBOL}`
      ),

      firstWorking(
        FUTURES_BASES,

        `/fapi/v1/openInterest` +
        `?symbol=${SYMBOL}`
      ),

      firstWorking(
        FUTURES_BASES,

        `/fapi/v1/fundingRate` +
        `?symbol=${SYMBOL}` +
        `&limit=1`
      ),

      firstWorking(
        FUTURES_BASES,

        `/futures/data/globalLongShortAccountRatio` +
        `?symbol=${SYMBOL}` +
        `&period=5m` +
        `&limit=1`
      )

    ]);


  const premium =
    premiumResult.status === 'fulfilled'
      ? premiumResult.value.data
      : null;


  const oi =
    oiResult.status === 'fulfilled'
      ? oiResult.value.data
      : null;


  const funding =
    fundingResult.status === 'fulfilled'
      ? fundingResult.value.data?.[0]
      : null;


  const longShort =
    longShortResult.status === 'fulfilled'
      ? longShortResult.value.data?.[0]
      : null;


  const result = {

    markPrice:
      finite(
        premium?.markPrice
      ),

    indexPrice:
      finite(
        premium?.indexPrice
      ),

    fundingRate:
      finite(
        premium?.lastFundingRate ??
        funding?.fundingRate
      ),

    nextFundingTime:
      premium?.nextFundingTime ??
      null,

    oi:
      finite(
        oi?.openInterest
      ),

    longShortRatio:
      finite(
        longShort?.longShortRatio
      ),

    source:
      'Binance Futures'

  };


  cacheSet(
    'futuresStats',
    result,
    5000
  );


  return result;

}


/* =========================================================
   LIQUIDATIONS
========================================================= */

async function liquidationStats() {

  const cached =
    cacheGet('liq');

  if (cached)
    return cached;

  try {

    const response =
      await firstWorking(

        FUTURES_BASES,

        `/fapi/v1/allForceOrders` +
        `?symbol=${SYMBOL}` +
        `&limit=100`

      );


    const cutoff =
      Date.now() -
      60 * 60 * 1000;


    let total = 0;
    let long = 0;
    let short = 0;


    for (
      const item of
      (response.data || [])
    ) {

      if (
        Number(item.time || 0) <
        cutoff
      ) {

        continue;

      }


      const quantity =
        Number(
          item.origQty || 0
        );

      const price =
        Number(
          item.averagePrice ||
          item.price ||
          0
        );


      const value =
        Math.abs(
          quantity *
          price
        );


      total += value;


      if (
        String(item.side)
          .toUpperCase() ===
        'SELL'
      ) {

        long += value;

      }


      if (
        String(item.side)
          .toUpperCase() ===
        'BUY'
      ) {

        short += value;

      }

    }


    const result = {

      total1h:
        total,

      long1h:
        long,

      short1h:
        short,

      bias:
        long >
        short * 1.15

          ? 'LONG LIQUIDATION HEAVY'

          : short >
            long * 1.15

            ? 'SHORT LIQUIDATION HEAVY'

            : 'BALANCED',

      source:
        'Binance Futures'

    };


    cacheSet(
      'liq',
      result,
      15000
    );


    return result;


  } catch {

    return {

      total1h:
        null,

      long1h:
        null,

      short1h:
        null,

      bias:
        'UNAVAILABLE',

      source:
        'Unavailable'

    };

  }

}


/* =========================================================
   FEAR & GREED
========================================================= */

async function fearGreed() {

  const cached =
    cacheGet('fg');

  if (cached)
    return cached;

  try {

    const data =
      await fetchJSON(
        'https://api.alternative.me/fng/?limit=1'
      );


    const item =
      data?.data?.[0];


    const result = {

      value:
        finite(
          item?.value
        ),

      classification:
        item?.value_classification ||
        null,

      source:
        'Alternative.me'

    };


    cacheSet(
      'fg',
      result,
      60000
    );


    return result;


  } catch {

    return {

      value:
        null,

      classification:
        null,

      source:
        'Unavailable'

    };

  }

}


/* =========================================================
   MACRO / GLOBAL MARKETS
========================================================= */

async function globalMarkets() {

  const cached =
    cacheGet('markets');

  if (cached)
    return cached;


  const symbols = {

    SP500:
      '%5EGSPC',

    NASDAQ:
      '%5EIXIC',

    DOW:
      '%5EDJI',

    VIX:
      '%5EVIX',

    DXY:
      'DX-Y.NYB'

  };


  const result = {};


  for (
    const [name, symbol]
    of Object.entries(symbols)
  ) {

    try {

      const data =
        await fetchJSON(

          `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}` +
          `?range=1d&interval=5m`,

          {},

          8000

        );


      const chart =
        data?.chart?.result?.[0];

      const quote =
        chart
          ?.indicators
          ?.quote?.[0];


      const closes =
        (quote?.close || [])
          .filter(
            x =>
              Number.isFinite(x)
          );


      const last =
        closes.at(-1);

      const previousClose =
        closes.at(-2);


      result[name] = {

        last:
          finite(last),

        change:
          previousClose
            ? (
                (
                  last -
                  previousClose
                ) /
                previousClose
              ) * 100
            : null,

        source:
          'Yahoo Finance'

      };


    } catch {

      result[name] = {

        last:
          null,

        change:
          null,

        source:
          'Unavailable'

      };

    }

  }


  cacheSet(
    'markets',
    result,
    30000
  );


  return result;

}


/* =========================================================
   ETF CONTEXT
========================================================= */

async function etfContext() {

  /*
    ETF data is DAILY context.
    We deliberately do NOT pretend it is tick-by-tick.
  */

  return {

    score:
      50,

    label:
      'ETF DATA UNAVAILABLE',

    source:
      'Farside daily context'

  };

}


/* =========================================================
   SNAPSHOT DELTA
========================================================= */

function snapshotDelta(
  key,
  value
) {

  const old =
    previous.get(key);


  previous.set(
    key,
    {
      value,
      time:
        Date.now()
    }
  );


  if (!old)
    return null;


  return pctChange(
    value,
    old.value
  );

}


/* =========================================================
   SIGNAL FACTORS
========================================================= */

function factorFromTrend(
  value
) {

  if (
    value ===
    'BULLISH'
  ) {

    return 80;

  }

  if (
    value ===
    'BEARISH'
  ) {

    return 20;

  }

  return 50;

}


function factorFromRSI(
  value
) {

  if (value == null)
    return 50;


  if (
    value >= 55 &&
    value <= 70
  ) {

    return 75;

  }


  if (
    value >= 30 &&
    value < 45
  ) {

    return 30;

  }


  if (value > 70)
    return 45;


  if (value < 30)
    return 55;


  return 50;

}


function factorFromRelation(
  price,
  ema20Value,
  ema50Value,
  vwapValue
) {

  let score = 50;


  if (
    price >
    ema20Value
  ) {

    score += 10;

  } else if (
    price <
    ema20Value
  ) {

    score -= 10;

  }


  if (
    ema20Value >
    ema50Value
  ) {

    score += 15;

  } else if (
    ema20Value <
    ema50Value
  ) {

    score -= 15;

  }


  if (
    price >
    vwapValue
  ) {

    score += 15;

  } else if (
    price <
    vwapValue
  ) {

    score -= 15;

  }


  return clamp(
    score,
    0,
    100
  );

}


/* =========================================================
   SIGNAL ENGINE
========================================================= */

function buildSignal({

  price,
  tech5,
  tech15,
  futures,
  liquidation,
  fg,
  etf

}) {

  const fprice =
    factorFromTrend(
      tech5.trend
    );


  const frsi =
    factorFromRSI(
      tech5.rsi
    );


  const fema =
    factorFromRelation(
      price,
      tech5.ema20,
      tech5.ema50,
      tech5.vwap
    );


  const fvol =
    50;


  /*
    OI is neutral until historical
    delta is warmed up.
  */

  const foi =
    futures.oi == null
      ? 50
      : 50;


  let ffund = 50;

  if (
    futures.fundingRate !=
    null
  ) {

    ffund =
      clamp(
        50 +
        futures.fundingRate *
        100000,

        0,
        100
      );

  }


  let fliq = 50;

  if (
    liquidation.long1h !=
    null &&
    liquidation.short1h !=
    null
  ) {

    if (
      liquidation.short1h >
      liquidation.long1h
    ) {

      fliq = 65;

    } else if (
      liquidation.long1h >
      liquidation.short1h
    ) {

      fliq = 35;

    }

  }


  const flarge =
    50;


  const fetf =
    etf.score;


  const fmacro =
    50;


  const ffg =
    fg.value == null
      ? 50
      : clamp(
          fg.value,
          0,
          100
        );


  /*
    SCORE WEIGHTS

    Price Structure 15
    RSI             10
    EMA + VWAP      10
    Volume          10
    Open Interest   15
    Funding          5
    Liquidation     10
    Large Trade     10
    ETF              5
    Macro            5
    Fear & Greed     5
  */

  const score =
    Math.round(

      fprice * 0.15 +

      frsi * 0.10 +

      fema * 0.10 +

      fvol * 0.10 +

      foi * 0.15 +

      ffund * 0.05 +

      fliq * 0.10 +

      flarge * 0.10 +

      fetf * 0.05 +

      fmacro * 0.05 +

      ffg * 0.05

    );


  let direction =
    'WAIT';


  if (
    score >= 62
  ) {

    direction =
      'BUY';

  } else if (
    score <= 38
  ) {

    direction =
      'SELL';

  }


  const atrValue =
    tech5.atr ||
    price * 0.003;


  let entry =
    price;

  let sl =
    null;

  let target1 =
    null;

  let target2 =
    null;


  if (
    direction ===
    'BUY'
  ) {

    sl =
      price -
      Math.max(
        atrValue * 1.1,
        price * 0.0015
      );


    target1 =
      price +
      (
        price -
        sl
      ) * 1.2;


    target2 =
      price +
      (
        price -
        sl
      ) * 2;


  } else if (
    direction ===
    'SELL'
  ) {

    sl =
      price +
      Math.max(
        atrValue * 1.1,
        price * 0.0015
      );


    target1 =
      price -
      (
        sl -
        price
      ) * 1.2;


    target2 =
      price -
      (
        sl -
        price
      ) * 2;

  }


  const confidence =
    Math.round(
      50 +
      Math.abs(
        score - 50
      ) * 1.35
    );


  const availableFactors = [

    tech5.rsi,
    tech5.ema20,
    tech5.ema50,
    tech5.vwap,
    futures.oi,
    futures.fundingRate,
    fg.value

  ].filter(
    x => x != null
  ).length;


  const coverage =
    Math.round(
      (
        availableFactors /
        7
      ) * 100
    );


  const reasons = [];


  reasons.push(
    `5m trend ${tech5.trend}`
  );


  if (
    tech5.rsi !=
    null
  ) {

    reasons.push(
      `RSI ${tech5.rsi.toFixed(1)}`
    );

  }


  reasons.push(
    `EMA/VWAP ${
      fema >= 55
        ? 'supportive'
        : 'weak'
    }`
  );


  if (
    futures.oi ==
    null
  ) {

    reasons.push(
      'OI unavailable'
    );

  }


  if (
    futures.fundingRate ==
    null
  ) {

    reasons.push(
      'Funding unavailable'
    );

  }


  if (
    fg.value !=
    null
  ) {

    reasons.push(
      `Fear & Greed ${fg.value}`
    );

  }


  return {

    score,

    direction,

    label:

      direction ===
      'BUY'

        ? '🟢 BUY — CONFIRMATION'

        : direction ===
          'SELL'

          ? '🔴 SELL — CONFIRMATION'

          : '🟡 WAIT — NO CLEAR EDGE',


    confidence,

    coverage,

    entry,

    sl,

    target1,

    target2,

    holding:

      direction ===
      'WAIT'

        ? 'Wait for confirmation'

        : '5–30 min scalp window',


    decision:

      direction ===
      'WAIT'

        ? 'No-trade / confirmation mode'

        : 'Decision support only — guaranteed profit नहीं।',


    reasons,


    factors: {

      price:
        fprice,

      rsi:
        frsi,

      ema:
        fema,

      volume:
        fvol,

      oi:
        foi,

      funding:
        ffund,

      liq:
        fliq,

      large:
        flarge,

      etf:
        fetf,

      macro:
        fmacro,

      fg:
        ffg

    }

  };

}


/* =========================================================
   DASHBOARD DATA
========================================================= */

async function dashboard() {

  const results =
    await Promise.allSettled([

      spot24h(),

      klines(
        '5m',
        120
      ),

      klines(
        '15m',
        120
      ),

      futuresStats(),

      liquidationStats(),

      fearGreed(),

      globalMarkets(),

      etfContext()

    ]);


  const [

    spotResult,
    k5Result,
    k15Result,
    futuresResult,
    liquidationResult,
    fgResult,
    marketsResult,
    etfResult

  ] = results;


  if (
    spotResult.status !==
    'fulfilled'
  ) {

    throw spotResult.reason;

  }


  if (
    k5Result.status !==
    'fulfilled'
  ) {

    throw k5Result.reason;

  }


  const spot =
    spotResult.value;


  const rows5 =
    k5Result.value;


  const rows15 =
    k15Result.status ===
    'fulfilled'

      ? k15Result.value

      : rows5;


  const futures =
    futuresResult.status ===
    'fulfilled'

      ? futuresResult.value

      : {

          markPrice:null,

          indexPrice:null,

          fundingRate:null,

          nextFundingTime:null,

          oi:null,

          longShortRatio:null,

          source:
            'Unavailable'

        };


  const liquidation =
    liquidationResult.status ===
    'fulfilled'

      ? liquidationResult.value

      : {

          total1h:null,

          long1h:null,

          short1h:null,

          bias:
            'UNAVAILABLE',

          source:
            'Unavailable'

        };


  const fg =
    fgResult.status ===
    'fulfilled'

      ? fgResult.value

      : {

          value:null,

          classification:null,

          source:
            'Unavailable'

        };


  const markets =
    marketsResult.status ===
    'fulfilled'

      ? marketsResult.value

      : {};


  const etf =
    etfResult.status ===
    'fulfilled'

      ? etfResult.value

      : {

          score:50,

          label:
            'ETF DATA UNAVAILABLE',

          source:
            'Unavailable'

        };


  const tech5 =
    technical(
      rows5
    );


  const tech15 =
    technical(
      rows15
    );


  const signal =
    buildSignal({

      price:
        spot.price,

      tech5,

      tech15,

      futures,

      liquidation,

      fg,

      etf

    });


  const oiDelta =
    snapshotDelta(
      'oi',
      futures.oi
    );


  const volumeDelta =
    snapshotDelta(
      'volume',
      spot.quoteVolume24h
    );


  const oiRows = [

    {
      tf:'1m',
      oi:futures.oi,
      change:oiDelta,
      volume:spot.volume24h,
      volumeChange:volumeDelta
    },

    {
      tf:'5m',
      oi:futures.oi,
      change:oiDelta,
      volume:spot.volume24h,
      volumeChange:volumeDelta
    },

    {
      tf:'10m',
      oi:futures.oi,
      change:oiDelta,
      volume:spot.volume24h,
      volumeChange:volumeDelta
    },

    {
      tf:'30m',
      oi:futures.oi,
      change:oiDelta,
      volume:spot.volume24h,
      volumeChange:volumeDelta
    },

    {
      tf:'1h',
      oi:futures.oi,
      change:oiDelta,
      volume:spot.volume24h,
      volumeChange:volumeDelta
    }

  ];


  return {

    ok:true,

    timestamp:
      new Date()
        .toISOString(),

    source:
      'Binance Spot + Binance Futures + public macro/sentiment sources',


    price:
      spot.price,

    change:
      spot.change,

    volume24h:
      spot.volume24h,


    technical:
      tech5,

    technical15:
      tech15,


    signal,


    oi:
      oiRows,


    funding: {

      rate:
        futures.fundingRate,

      nextFundingTime:
        futures.nextFundingTime

    },


    longShort:

      futures.longShortRatio ==
      null

        ? null

        : {
            ratio:
              futures.longShortRatio
          },


    liquidation,


    markets,


    sessions: {

      India:'OPEN',

      UK:'OPEN',

      USA:'OPEN',

      China:'OPEN',

      note:
        'Session labels are informational.'

    },


    fearGreed: {

      value:
        fg.value,

      classification:
        fg.classification

    },


    sourceLinks: [

      {
        name:
          'Binance Spot',

        url:
          'https://www.binance.com/en/markets'
      },

      {
        name:
          'Binance Futures',

        url:
          'https://www.binance.com/en/futures'
      },

      {
        name:
          'Alternative.me Fear & Greed',

        url:
          'https://alternative.me/crypto/fear-and-greed-index/'
      },

      {
        name:
          'Farside ETF',

        url:
          'https://farside.co.uk/btc/'
      },

      {
        name:
          'TradingView',

        url:
          'https://www.tradingview.com/symbols/BTCUSDT/'
      }

    ],


    news: [],


    sources: [

      {
        name:
          'Binance Spot Price',

        status:
          'LIVE'
      },

      {
        name:
          'Binance Futures OI',

        status:
          futures.oi != null
            ? 'LIVE'
            : 'UNAVAILABLE'
      },

      {
        name:
          'Binance Funding',

        status:
          futures.fundingRate != null
            ? 'LIVE'
            : 'UNAVAILABLE'
      },

      {
        name:
          'Liquidations',

        status:
          liquidation.total1h != null
            ? 'LIVE'
            : 'UNAVAILABLE'
      },

      {
        name:
          'Fear & Greed',

        status:
          fg.value != null
            ? 'LIVE'
            : 'UNAVAILABLE'
      },

      {
        name:
          'Macro / Indices',

        status:
          Object.keys(markets).length
            ? 'LIVE'
            : 'UNAVAILABLE'
      },

      {
        name:
          'ETF Flow',

        status:
          'DAILY CONTEXT / NOT TICK DATA'
      }

    ],


    stream: {

      connected:
        streamConnected,

      lastMessage:
        streamLastMessage,

      reconnects:
        streamReconnects

    }

  };

}


/* =========================================================
   HEALTH
========================================================= */

app.get(
  '/api/health',
  (req,res) => {

    res.json({

      ok:true,

      service:
        'BTC/USD AI SCALPING ENGINE V5',

      time:
        new Date()
          .toISOString(),

      websocket: {

        connected:
          streamConnected,

        lastMessage:
          streamLastMessage,

        reconnects:
          streamReconnects

      },

      uptime:
        process.uptime(),

      node:
        process.version

    });

  }
);


/* =========================================================
   PRICE API
========================================================= */

app.get(
  '/api/price',
  async (req,res) => {

    try {

      const price =
        await spot24h();

      const futures =
        await futuresStats();


      res.json({

        symbol:
          SYMBOL,

        lastPrice:
          price.price,

        markPrice:
          futures.markPrice,

        indexPrice:
          futures.indexPrice,

        fundingRate:
          futures.fundingRate,

        nextFundingTime:
          futures.nextFundingTime,

        volume24h:
          price.volume24h,

        turnover24h:
          price.quoteVolume24h,

        price24hPcnt:
          price.change != null
            ? price.change / 100
            : null,

        source:
          'Binance'

      });


    } catch (error) {

      res.status(503)
        .json({

          ok:false,

          error:
            error.message

        });

    }

  }
);


/* =========================================================
   MAIN DASHBOARD API
========================================================= */

app.get(
  '/api/dashboard',
  async (req,res) => {

    try {

      const data =
        await dashboard();

      res.json(data);


    } catch (error) {

      console.error(
        'Dashboard error:',
        error
      );


      res.status(503)
        .json({

          ok:false,

          error:
            error.message ||
            'Dashboard data unavailable'

        });

    }

  }
);


/* =========================================================
   OPEN INTEREST API
========================================================= */

app.get(
  '/api/oi',
  async (req,res) => {

    try {

      const futures =
        await futuresStats();


      res.json({

        ok:true,

        symbol:
          SYMBOL,

        openInterest:
          futures.oi,

        fundingRate:
          futures.fundingRate,

        longShortRatio:
          futures.longShortRatio,

        source:
          'Binance Futures'

      });


    } catch (error) {

      res.status(503)
        .json({

          ok:false,

          error:
            error.message

        });

    }

  }
);


/* =========================================================
   FUNDING API
========================================================= */

app.get(
  '/api/funding',
  async (req,res) => {

    try {

      const futures =
        await futuresStats();


      res.json({

        ok:true,

        symbol:
          SYMBOL,

        fundingRate:
          futures.fundingRate,

        nextFundingTime:
          futures.nextFundingTime,

        source:
          'Binance Futures'

      });


    } catch (error) {

      res.status(503)
        .json({

          ok:false,

          error:
            error.message

        });

    }

  }
);


/* =========================================================
   EXPRESS 5 SAFE FRONTEND FALLBACK
========================================================= */

/*
  IMPORTANT:

  Do NOT use:

  app.get('*', ...)

  because Express 5 / path-to-regexp
  can reject that route.

  This middleware is compatible.
*/

app.use(
  (req,res) => {

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
   BINANCE WEBSOCKET
========================================================= */

function startStream() {

  if (ws) {

    try {
      ws.close();
    } catch {}

  }


  const url =
    'wss://stream.binance.com:9443/ws/btcusdt@ticker';


  try {

    ws =
      new WebSocket(url);


    ws.on(
      'open',
      () => {

        streamConnected =
          true;

        console.log(
          'Binance WebSocket connected'
        );

      }
    );


    ws.on(
      'message',
      () => {

        streamLastMessage =
          Date.now();

      }
    );


    ws.on(
      'close',
      () => {

        streamConnected =
          false;

        scheduleStream();

      }
    );


    ws.on(
      'error',
      error => {

        streamConnected =
          false;

        console.error(
          'WebSocket error:',
          error.message
        );

        try {
          ws.close();
        } catch {}

      }
    );


  } catch {

    streamConnected =
      false;

    scheduleStream();

  }

}


/* =========================================================
   WEBSOCKET RECONNECT
========================================================= */

function scheduleStream() {

  if (wsTimer)
    return;


  streamReconnects++;


  wsTimer =
    setTimeout(
      () => {

        wsTimer =
          null;

        startStream();

      },
      5000
    );

}


/* =========================================================
   START SERVER
========================================================= */

app.listen(
  PORT,
  HOST,
  () => {

    console.log(
      `BTC AI SCALPING ENGINE V5 listening on ${HOST}:${PORT}`
    );

    startStream();

  }
);
