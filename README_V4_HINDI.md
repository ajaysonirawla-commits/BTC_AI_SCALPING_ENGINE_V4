# BTC/USD AI SCALPING ENGINE V4

यह V4 package वर्तमान V3.1 dashboard को online-ready बनाने के लिए है। पुराने versions को चलाने की जरूरत नहीं है।

## इसमें क्या है
- Binance Spot live BTC price / OHLC / 24h volume
- Binance Futures live OI, funding, long/short ratio
- Binance WebSocket liquidation + aggregate-trade proxy
- RSI(14), VWAP, EMA20, EMA50, ATR, 5m/15m trend
- 0–100 decision-support score, confidence, coverage
- Entry / Stop Loss / Target 1 / Target 2 (केवल LONG/SHORT पर)
- OI history warm-up; पर्याप्त snapshots से पहले change को fabricated नहीं दिखाया जाता
- Fear & Greed (Alternative.me)
- Farside BTC ETF daily context
- Yahoo public market context
- Crypto news context + source links
- Alert history / read-unread / browser beep
- TradingView BTCUSDT chart with RSI / Volume / VWAP
- Mobile responsive layout
- /api/health health check for hosting

## Local test
1. Node.js 24.x install करें.
2. इस folder में CMD खोलें.
3. `npm install`
4. `npm start`
5. Chrome में `http://localhost:3000`

## Online deploy — Render
1. इस पूरे folder को GitHub repository में upload करें.
2. Render → New → Web Service.
3. GitHub repository चुनें.
4. Build Command: `npm install`
5. Start Command: `npm start`
6. Health Check Path: `/api/health`
7. Deploy करें.
8. Render आपको `https://<name>.onrender.com` URL देगा.
9. यही URL फोन और दूसरे laptop में Chrome से खोलें.

## महत्वपूर्ण
- `.env` या API keys GitHub में upload न करें.
- Current V4 में CoinGlass required नहीं है.
- Signal decision-support है; guaranteed profit नहीं.
- ETF flow daily context है, tick-by-tick institutional order flow नहीं.
- Large Trade Proxy wallet-level whale tracker नहीं है.
- OI Δ% local server snapshots से बनता है; नया deployment शुरू होने पर history फिर से warm-up होगी.
