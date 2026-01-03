#!/bin/bash
# SMC HYBRID PRODUCTION LAUNCHER

echo "==========================================="
echo "   SMC BOT v13.2.1 - PRODUCTION MODE"
echo "==========================================="

if [ ! -f .env ]; then
    echo "❌ ERROR: .env file missing!"
    exit 1
fi

mkdir -p logs data

echo "🚀 Starting Bot..."
echo "📝 Logging to logs/bot.log"

while true; do
    python3 bot.py >> logs/bot.log 2>&1
    
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "🛑 Bot stopped manually."
        break
    else
        echo "⚠️  Crashed (Code $EXIT_CODE). Restarting in 5s..."
        sleep 5
    fi
done