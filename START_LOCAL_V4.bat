@echo off
cd /d "%~dp0"
if not exist node_modules (
  echo Installing dependencies...
  npm install
)
echo Starting BTC AI SCALPING ENGINE V4...
npm start
pause
