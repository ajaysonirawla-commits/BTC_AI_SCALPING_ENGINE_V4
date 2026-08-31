require('dotenv').config();

const express = require('express');
const cors = require('cors');
const path = require('path');
const WebSocket = require('ws');

const app = express();

app.use(cors());
app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, 'frontend')
  )
);

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';

const SYMBOL = 'BTCUSDT';

const PROVIDERS = {
  bybit: 'https://api.bybit.com',
  binance: 'https://api.binance.com',
  binanceVision: 'https://data-api.binance.vision'
};

const state = {
  startedAt: Date.now(),

  stream: {
    connected: false,
    lastMessage: null,
    reconnects: 0,
    provider: 'Bybit'
  },

  last: null,
  lastSignalKey: '',
  history: new Map()
};


/* =========================================================
   BASIC HELPERS
========================================================= */

async function fetchJson(url, timeout = 12000) {

  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    timeout
  );

  try {

    const response = await fetch(
      url,
      {
        method: 'GET',

        headers: {
          accept: 'application/json',
          'user-agent':
            'BTC-AI-Scalping-Engine/4.0'
        },

        signal: controller.signal
      }
    );

    const text = await response.text();

    let data;

    try {

      data = JSON.parse(text);

    } catch (e) {

      throw new Error(
        `Non-JSON response from ${new URL(url).hostname}`
      );

    }

    if (!response.ok) {

      throw new Error(
        `HTTP ${response.status}`
      );

    }

    return data;

  } finally {

    clearTimeout(timer);

  }
}


function n(value, fallback = null) {

  const x = Number(value);

  return Number.isFinite(x)
    ? x
    : fallback;
}


function clamp(value, min = 0, max = 100) {

  return Math.max(
    min,
    Math.min(max, value)
  );

}


function avg(values) {

  return values.length
    ? values.reduce(
        (a, b) => a + b,
        0
      ) / values.length
    : null;

}


function pct(current, previous) {

  if (
    current == null ||
    previous == null ||
    previous === 0
  ) {
    return null;
  }

  return (
    (current - previous) /
    previous
  ) * 100;

}


function nowISO() {

  return new Date().toISOString();

}


/* =========================================================
   TECHNICAL INDICATORS
========================================================= */

function ema(values, period) {

  if (!values.length) {
    return null;
  }

  const multiplier =
    2 / (period + 1);

  let result = values[0];

  for (
    let i = 1;
    i < values.length;
    i++
  ) {

    result =
      values[i] * multiplier +
      result * (1 - multiplier);

  }

  return result;

}


function rsi(values, period = 14) {

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

    const difference =
      values[i] -
      values[i - 1];

    if (difference >= 0) {

      gain += difference;

    } else {

      loss -= difference;

    }

  }

  let averageGain =
    gain / period;

  let averageLoss =
    loss / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {

    const difference =
      values[i] -
      values[i - 1];

    averageGain =
      (
        averageGain *
          (period - 1) +
        (difference > 0
          ? difference
          : 0)
      ) / period;

    averageLoss =
      (
        averageLoss *
          (period - 1) +
        (difference < 0
          ? -difference
          : 0)
      ) / period;

  }

  if (averageLoss === 0) {

    return 100;

  }

  const rs =
    averageGain /
    averageLoss;

  return 100 -
    (100 / (1 + rs));

}


function atr(rows, period = 14) {

  if (
    rows.length <= period
  ) {
    return null;
  }

  const trueRanges = [];

  for (
    let i = 1;
    i < rows.length;
    i++
  ) {

    const high =
      rows[i].high;

    const low =
      rows[i].low;

    const previousClose =
      rows[i - 1].close;

    trueRanges.push(
      Math.max(
        high - low,

        Math.abs(
          high - previousClose
        ),

        Math.abs(
          low - previousClose
        )
      )
    );

  }

  return avg(
    trueRanges.slice(-period)
  );

}


function sessionVWAP(rows) {

  let priceVolume = 0;
  let volume = 0;

  for (const row of rows) {

    const typicalPrice =
      (
        row.high +
        row.low +
        row.close
      ) / 3;

    priceVolume +=
      typicalPrice *
      row.volume;

    volume += row.volume;

  }

  return volume
    ? priceVolume / volume
    : null;

}


/* =========================================================
   BYBIT KLINE
========================================================= */

function parseBybitKlines(list) {

  return (
    Array.isArray(list)
      ? list
      : []
  )
    .slice()
    .reverse()
    .map(row => ({

      time: n(row[0]),

      open: n(row[1]),

      high: n(row[2]),

      low: n(row[3]),

      close: n(row[4]),

      volume: n(row[5])

    }))
    .filter(row =>
      [
        row.open,
        row.high,
        row.low,
        row.close,
        row.volume
      ].every(
        value => value != null
      )
    );

}


/* =========================================================
   BYBIT TICKER
========================================================= */

async function bybitTicker() {

  const url =
    `${PROVIDERS.bybit}` +
    `/v5/market/tickers` +
    `?category=linear` +
    `&symbol=${SYMBOL}`;

  const data =
    await fetchJson(url);

  const item =
    data?.result?.list?.[0];

  if (!item) {

    throw new Error(
      'Bybit ticker unavailable'
    );

  }

  return {

    price:
      n(item.lastPrice),

    change:
      n(item.price24hPcnt) * 100,

    volume24h:
      n(item.volume24h),

    turnover24h:
      n(item.turnover24h),

    fundingRate:
      n(item.fundingRate),

    nextFundingTime:
      n(item.nextFundingTime),

    markPrice:
      n(item.markPrice),

    indexPrice:
      n(item.indexPrice)

  };

}


/* =========================================================
   BYBIT KLINES
========================================================= */

async function bybitKlines(
  interval = '5',
  limit = 200
) {

  const url =
    `${PROVIDERS.bybit}` +
    `/v5/market/kline` +
    `?category=linear` +
    `&symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${limit}`;

  const data =
    await fetchJson(url);

  return parseBybitKlines(
    data?.result?.list
  );

}


/* =========================================================
   BYBIT OPEN INTEREST
========================================================= */

async function bybitOI(
  interval = '5min'
) {

  const url =
    `${PROVIDERS.bybit}` +
    `/v5/market/open-interest` +
    `?category=linear` +
    `&symbol=${SYMBOL}` +
    `&intervalTime=${interval}` +
    `&limit=50`;

  const data =
    await fetchJson(url);

  return (
    data?.result?.list || []
  )
    .slice()
    .reverse()
    .map(item => ({

      time:
        n(item.timestamp),

      oi:
        n(
          item.singleOpenInterest ??
          item.openInterest
        )

    }));

}


/* =========================================================
   BYBIT LONG SHORT
========================================================= */

async function bybitLongShort() {

  const url =
    `${PROVIDERS.bybit}` +
    `/v5/market/account-ratio` +
    `?category=linear` +
    `&symbol=${SYMBOL}` +
    `&period=5min` +
    `&limit=1`;

  const data =
    await fetchJson(url);

  const item =
    data?.result?.list?.[0];

  if (!item) {

    return null;

  }

  const buyRatio =
    n(item.buyRatio);

  const sellRatio =
    n(item.sellRatio);

  return {

    buyRatio,

    sellRatio,

    ratio:
      buyRatio /
      Math.max(
        sellRatio || 1,
        0.0000001
      )

  };

}


/* =========================================================
   BINANCE FALLBACK
   HTTP 451 होने पर Bybit primary रहेगा
========================================================= */

async function binanceSpotFallback() {

  const urls = [

    `${PROVIDERS.binance}` +
    `/api/v3/ticker/24hr` +
    `?symbol=${SYMBOL}`,

    `${PROVIDERS.binanceVision}` +
    `/api/v3/ticker/24hr` +
    `?symbol=${SYMBOL}`

  ];

  for (const url of urls) {

    try {

      const item =
        await fetchJson(
          url,
          7000
        );

      return {

        price:
          n(item.lastPrice),

        change:
          n(item.priceChangePercent),

        volume24h:
          n(item.volume),

        turnover24h:
          n(item.quoteVolume),

        provider:
          'Binance'

      };

    } catch (error) {

      console.log(
        'Binance fallback failed:',
        error.message
      );

    }

  }

  return null;

}


/* =========================================================
   TECHNICAL DATA
========================================================= */

async function getTechnicals() {

  const [
    rows5,
    rows15
  ] = await Promise.all([

    bybitKlines(
      '5',
      200
    ),

    bybitKlines(
      '15',
      200
    )

  ]);

  const closes5 =
    rows5.map(
      row => row.close
    );

  const vwap =
    sessionVWAP(
      rows5.slice(-96)
    );

  const ema20 =
    ema(
      closes5.slice(-100),
      20
    );

  const ema50 =
    ema(
      closes5.slice(-150),
      50
    );

  const rsiValue =
    rsi(
      closes5,
      14
    );

  const atrValue =
    atr(
      rows5,
      14
    );

  const recentVolume =
    avg(
      rows5
        .slice(-5)
        .map(
          row => row.volume
        )
    );

  const previousVolume =
    avg(
      rows5
        .slice(-25, -5)
        .map(
          row => row.volume
        )
    );

  let trend = 'MIXED';

  if (
    ema20 &&
    ema50 &&
    closes5.length
  ) {

    const price =
      closes5.at(-1);

    if (
      ema20 > ema50 &&
      price > ema20
    ) {

      trend = 'BULLISH';

    } else if (
      ema20 < ema50 &&
      price < ema20
    ) {

      trend = 'BEARISH';

    }

  }

  const closes15 =
    rows15.map(
      row => row.close
    );

  const ema20_15 =
    ema(
      closes15.slice(-100),
      20
    );

  const ema50_15 =
    ema(
      closes15.slice(-150),
      50
    );

  let trend15 = 'MIXED';

  if (
    ema20_15 &&
    ema50_15 &&
    closes15.length
  ) {

    const price15 =
      closes15.at(-1);

    if (
      ema20_15 > ema50_15 &&
      price15 > ema20_15
    ) {

      trend15 = 'BULLISH';

    } else if (
      ema20_15 < ema50_15 &&
      price15 < ema20_15
    ) {

      trend15 = 'BEARISH';

    }

  }

  return {

    rows5,

    rows15,

    technical: {

      rsi:
        rsiValue,

      vwap,

      ema20,

      ema50,

      atr:
        atrValue,

      trend,

      volumeRatio:
        previousVolume
          ? recentVolume /
            previousVolume
          : null

    },

    technical15: {

      trend:
        trend15,

      ema20:
        ema20_15,

      ema50:
        ema50_15

    }

  };

}


/* =========================================================
   SIGNAL FACTORS
========================================================= */

function calculateFactors({

  price,

  technical,

  oiDelta,

  funding,

  longShort

}) {

  const priceScore =
    technical.trend === 'BULLISH'
      ? 75
      : technical.trend === 'BEARISH'
        ? 25
        : 50;


  let rsiScore = 50;

  if (
    technical.rsi != null
  ) {

    if (
      technical.rsi < 30
    ) {

      rsiScore = 80;

    } else if (
      technical.rsi > 70
    ) {

      rsiScore = 20;

    } else {

      rsiScore =
        50 +
        (
          50 -
          technical.rsi
        ) * 0.5;

    }

  }


  let emaScore = 50;

  if (
    technical.vwap &&
    price
  ) {

    emaScore +=
      price >
      technical.vwap
        ? 20
        : -20;

  }

  if (
    technical.ema20 &&
    technical.ema50
  ) {

    emaScore +=
      technical.ema20 >
      technical.ema50
        ? 15
        : -15;

  }

  emaScore =
    clamp(emaScore);


  const volumeScore =
    technical.volumeRatio == null
      ? 50
      : clamp(
          50 +
          (
            technical.volumeRatio -
            1
          ) * 35
        );


  const oiScore =
    oiDelta == null
      ? 50
      : clamp(
          50 +
          oiDelta * 5
        );


  const fundingScore =
    funding == null
      ? 50
      : clamp(
          50 -
          funding * 10000
        );


  const liquidationScore =
    50;


  const largeTradeScore =
    longShort?.ratio > 1.10
      ? 60
      : longShort?.ratio < 0.90
        ? 40
        : 50;


  return {

    price:
      priceScore,

    rsi:
      rsiScore,

    ema:
      emaScore,

    volume:
      volumeScore,

    oi:
      oiScore,

    funding:
      fundingScore,

    liq:
      liquidationScore,

    large:
      largeTradeScore,

    etf:
      50,

    macro:
      50,

    fg:
      50

  };

}


/* =========================================================
   SIGNAL ENGINE 0–100
========================================================= */

function makeSignal(
  price,
  technical,
  factors,
  atrValue
) {

  const weights = {

    price: 15,

    rsi: 10,

    ema: 10,

    volume: 10,

    oi: 15,

    funding: 5,

    liq: 10,

    large: 10,

    etf: 5,

    macro: 5,

    fg: 5

  };


  let weightedScore = 0;

  let totalWeight = 0;


  for (
    const [
      key,
      weight
    ] of Object.entries(weights)
  ) {

    const value =
      n(factors[key]);

    if (
      value != null
    ) {

      weightedScore +=
        value * weight;

      totalWeight +=
        weight;

    }

  }


  const finalScore =
    totalWeight
      ? weightedScore /
        totalWeight
      : 50;


  let direction =
    'WAIT';


  if (
    finalScore >= 60
  ) {

    direction =
      'LONG';

  } else if (
    finalScore <= 40
  ) {

    direction =
      'SHORT';

  }


  const atrValueSafe =
    atrValue ||
    price * 0.003;


  const entry =
    price;


  let sl = null;
  let target1 = null;
  let target2 = null;


  if (
    direction === 'LONG'
  ) {

    sl =
      price -
      atrValueSafe * 1.2;

    target1 =
      price +
      atrValueSafe * 1.5;

    target2 =
      price +
      atrValueSafe * 2.5;

  }


  if (
    direction === 'SHORT'
  ) {

    sl =
      price +
      atrValueSafe * 1.2;

    target1 =
      price -
      atrValueSafe * 1.5;

    target2 =
      price -
      atrValueSafe * 2.5;

  }


  const confidence =
    Math.round(
      Math.abs(
        finalScore - 50
      ) * 2
    );


  const reasons = [];


  reasons.push(
    `Price structure ${technical.trend}`
  );


  if (
    technical.rsi != null
  ) {

    reasons.push(
      `RSI ${technical.rsi.toFixed(1)}`
    );

  }


  if (
    technical.vwap
  ) {

    reasons.push(
      price >
      technical.vwap
        ? 'Price above VWAP'
        : 'Price below VWAP'
    );

  }


  if (
    technical.volumeRatio != null
  ) {

    reasons.push(
      `Volume ${technical.volumeRatio.toFixed(2)}x`
    );

  }


  return {

    score:
      Math.round(
        clamp(finalScore)
      ),

    direction,

    label:
      direction === 'LONG'
        ? '🟢 LONG — TRADE SETUP'
        : direction === 'SHORT'
          ? '🔴 SHORT — TRADE SETUP'
          : '🟡 WAIT — CONFIRMATION NEEDED',

    confidence,

    coverage:
      Math.round(totalWeight),

    entry,

    sl,

    target1,

    target2,

    holding:
      direction === 'WAIT'
        ? 'Wait for confirmation'
        : '5–30 min scalp window',

    decision:
      'Decision support only — guaranteed profit नहीं।',

    reasons,

    factors

  };

}


/* =========================================================
   COMPLETE DASHBOARD DATA
========================================================= */

async function getDashboardData() {

  let ticker = null;

  let provider =
    'Bybit';


  try {

    ticker =
      await bybitTicker();

  } catch (error) {

    console.log(
      'Bybit ticker error:',
      error.message
    );

    ticker =
      await binanceSpotFallback();

    provider =
      ticker
        ? 'Binance'
        : 'Unavailable';

  }


  if (!ticker) {

    throw new Error(
      'BTC market data unavailable from Bybit and Binance fallback'
    );

  }


  const {

    rows5,

    rows15,

    technical,

    technical15

  } = await getTechnicals();


  const oiData =
    await bybitOI(
      '5min'
    ).catch(
      () => []
    );


  const oiNow =
    oiData.at(-1)?.oi ??
    null;


  const oiPrevious =
    oiData.at(-2)?.oi ??
    null;


  const oiChange =
    pct(
      oiNow,
      oiPrevious
    );


  const longShort =
    await bybitLongShort()
      .catch(
        () => null
      );


  const factors =
    calculateFactors({

      price:
        ticker.price,

      technical,

      oiDelta:
        oiChange,

      funding:
        ticker.fundingRate,

      longShort

    });


  const signal =
    makeSignal(

      ticker.price,

      technical,

      factors,

      technical.atr

    );


  const recentVolume =
    avg(
      rows5
        .slice(-5)
        .map(
          x => x.volume
        )
    );


  const previousVolume =
    avg(
      rows5
        .slice(-25, -5)
        .map(
          x => x.volume
        )
    );


  const volumeChange =
    pct(
      recentVolume,
      previousVolume
    );


  const oiRows = [

    '1m',

    '5m',

    '10m',

    '30m',

    '1h'

  ].map(tf => ({

    tf,

    oi:
      oiNow,

    change:
      oiChange,

    volume:
      recentVolume,

    volumeChange

  }));


  const data = {

    ok:
      true,

    timestamp:
      nowISO(),

    source:
      'Bybit primary / Binance fallback',

    price:
      ticker.price,

    change:
      ticker.change,

    volume24h:
      ticker.volume24h,

    turnover24h:
      ticker.turnover24h,


    technical,

    technical15,


    signal,


    oi:
      oiRows,


    openInterest: {

      btc:
        oiNow,

      usd:
        null,

      change5m:
        oiChange,

      source:
        'Bybit'

    },


    funding: {

      rate:
        ticker.fundingRate,

      nextFundingTime:
        ticker.nextFundingTime,

      source:
        'Bybit'

    },


    longShort,


    liquidation: {

      total1h:
        null,

      long1h:
        null,

      short1h:
        null,

      bias:
        'UNAVAILABLE'

    },


    markets: {

      BTCUSDT: {

        last:
          ticker.price,

        change:
          ticker.change

      }

    },


    sessions: {

      India:
        'OPEN',

      UK:
        'OPEN',

      USA:
        'OPEN',

      China:
        'OPEN',

      note:
        'Crypto trades 24/7; index sessions are indicative only.'

    },


    sourceLinks: [

      {

        name:
          'TradingView',

        url:
          'https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSDT'

      },

      {

        name:
          'Bybit',

        url:
          'https://www.bybit.com'

      },

      {

        name:
          'Binance',

        url:
          'https://www.binance.com'

      }

    ],


    news: [],


    sources: [

      {

        name:
          'Bybit market data',

        status:
          'LIVE'

      },

      {

        name:
          'Binance fallback',

        status:
          'READY'

      },

      {

        name:
          'Open Interest',

        status:
          oiNow != null
            ? 'LIVE'
            : 'UNAVAILABLE'

      },

      {

        name:
          'Funding',

        status:
          ticker.fundingRate != null
            ? 'LIVE'
            : 'UNAVAILABLE'

      },

      {

        name:
          'Liquidations',

        status:
          'UNAVAILABLE without dedicated feed'

      },

      {

        name:
          'ETF/Macro/Fear & Greed',

        status:
          'NOT CONNECTED'

      }

    ],


    stream: {

      connected:
        true,

      lastMessage:
        state.stream.lastMessage,

      reconnects:
        state.stream.reconnects,

      provider

    }

  };


  state.last =
    data;


  return data;

}


/* =========================================================
   HEALTH
========================================================= */

app.get(
  '/api/health',
  (req, res) => {

    res.json({

      ok:
        true,

      service:
        'BTC/USD AI SCALPING ENGINE V4',

      time:
        nowISO(),

      WebSocket: {

        connected:
          state.stream.connected,

        lastMessage:
          state.stream.lastMessage,

        reconnects:
          state.stream.reconnects

      },

      provider:
        state.stream.provider,

      lastError:
        null,

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
  async (req, res) => {

    try {

      const data =
        await getDashboardData();


      res.json({

        symbol:
          SYMBOL,

        lastPrice:
          data.price,

        markPrice:
          data.price,

        indexPrice:
          null,

        fundingRate:
          data.funding.rate,

        nextFundingTime:
          data.funding.nextFundingTime,

        volume24h:
          data.volume24h,

        turnover24h:
          data.turnover24h,

        price24hPcnt:
          data.change,

        source:
          data.stream.provider

      });

    } catch (error) {

      res.status(503).json({

        error:
          error.message

      });

    }

  }
);


/* =========================================================
   DASHBOARD API
========================================================= */

app.get(
  '/api/dashboard',
  async (req, res) => {

    try {

      const data =
        await getDashboardData();

      res.json(data);

    } catch (error) {

      console.error(
        'Dashboard API error:',
        error.message
      );

      res.status(503).json({

        ok:
          false,

        error:
          error.message,

        timestamp:
          nowISO()

      });

    }

  }
);


/* =========================================================
   SIGNAL HISTORY
========================================================= */

app.get(
  '/api/history',
  (req, res) => {

    res.json({

      ok:
        true,

      items:
        [
          ...state.history.values()
        ]
          .slice(-150)
          .reverse()

    });

  }
);


/* =========================================================
   FRONTEND FALLBACK
   IMPORTANT:
   NO app.get('*')
========================================================= */

app.use(
  (req, res, next) => {

    if (
      req.path.startsWith('/api/')
    ) {

      return res
        .status(404)
        .json({

          error:
            'API endpoint not found'

        });

    }

    next();

  }
);


app.get(
  '/',
  (req, res) => {

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
   START SERVER
========================================================= */

const server =
  app.listen(
    PORT,
    HOST,
    () => {

      console.log(
        `BTC AI Scalping Engine listening on ${HOST}:${PORT}`
      );

    }
  );


/* =========================================================
   BYBIT WEBSOCKET
========================================================= */

let ws = null;


function connectWebSocket() {

  try {

    ws =
      new WebSocket(
        'wss://stream.bybit.com/v5/public/linear'
      );


    ws.on(
      'open',
      () => {

        state.stream.connected =
          true;

        state.stream.provider =
          'Bybit';


        console.log(
          'Bybit WebSocket connected'
        );


        ws.send(
          JSON.stringify({

            op:
              'subscribe',

            args: [

              'tickers.BTCUSDT'

            ]

          })
        );

      }
    );


    ws.on(
      'message',
      message => {

        state.stream.lastMessage =
          Date.now();


        try {

          const data =
            JSON.parse(
              message.toString()
            );


          const price =
            data?.data?.lastPrice;


          if (price) {

            state.lastPrice =
              n(price);

          }

        } catch (error) {

        }

      }
    );


    ws.on(
      'close',
      () => {

        state.stream.connected =
          false;

        state.stream.reconnects++;


        console.log(
          'Bybit WebSocket closed. Reconnecting...'
        );


        setTimeout(
          connectWebSocket,
          5000
        );

      }
    );


    ws.on(
      'error',
      error => {

        state.stream.connected =
          false;

        console.log(
          'WebSocket error:',
          error.message
        );

      }
    );


  } catch (error) {

    state.stream.connected =
      false;

    setTimeout(
      connectWebSocket,
      5000
    );

  }

}


connectWebSocket();


/* =========================================================
   SIGNAL HISTORY AUTO UPDATE
========================================================= */

setInterval(
  async () => {

    try {

      const data =
        await getDashboardData();


      const signal =
        data.signal;


      if (
        signal &&
        signal.direction !== 'WAIT'
      ) {

        const key =
          signal.direction +
          '-' +
          Math.floor(
            Date.now() / 60000
          );


        if (
          key !==
          state.lastSignalKey
        ) {

          state.lastSignalKey =
            key;


          state.history.set(
            key,
            {

              id:
                Date.now(),

              direction:
                signal.direction,

              label:
                signal.label,

              score:
                signal.score,

              confidence:
                signal.confidence,

              entry:
                signal.entry,

              sl:
                signal.sl,

              target1:
                signal.target1,

              target2:
                signal.target2,

              time:
                nowISO(),

              read:
                false

            }
          );


          while (
            state.history.size >
            150
          ) {

            state.history.delete(
              state.history
                .keys()
                .next()
                .value
            );

          }

        }

      }

    } catch (error) {

      console.log(
        'Signal history update:',
        error.message
      );

    }

  },
  10000
);


/* =========================================================
   GRACEFUL SHUTDOWN
========================================================= */

process.on(
  'SIGTERM',
  () => {

    try {

      ws?.close();

    } catch (e) {}


    server.close(
      () => process.exit(0)
    );

  }
);
