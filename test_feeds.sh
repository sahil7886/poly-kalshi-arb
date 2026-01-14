#!/bin/bash
# Quick test script to check if Kalshi and Polymarket APIs are returning data

set -e

echo "🔍 Testing Data Feeds"
echo "====================="
echo ""

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "📊 Testing Kalshi API (BTC 15m markets)..."
echo "-------------------------------------------"

# Test Kalshi markets endpoint (no auth needed for public data)
KALSHI_RESPONSE=$(curl -s "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXBTC15M&limit=5")

echo "Response:"
echo "$KALSHI_RESPONSE" | jq '.' 2>/dev/null || echo "$KALSHI_RESPONSE"

MARKET_COUNT=$(echo "$KALSHI_RESPONSE" | jq '.markets | length' 2>/dev/null || echo "0")
echo ""
echo "✅ Found $MARKET_COUNT Kalshi BTC15M markets"

if [ "$MARKET_COUNT" -gt "0" ]; then
    FIRST_TICKER=$(echo "$KALSHI_RESPONSE" | jq -r '.markets[0].ticker' 2>/dev/null)
    echo "   First market ticker: $FIRST_TICKER"
fi

echo ""
echo "🟣 Testing Polymarket API (BTC 15m markets)..."
echo "-----------------------------------------------"

# Test Polymarket Gamma API
POLY_RESPONSE=$(curl -s "https://gamma-api.polymarket.com/markets?slug=btc-up-or-down-15m&limit=5")

echo "Response:"
echo "$POLY_RESPONSE" | jq '.' 2>/dev/null || echo "$POLY_RESPONSE"

POLY_COUNT=$(echo "$POLY_RESPONSE" | jq '. | length' 2>/dev/null || echo "0")
echo ""
echo "✅ Found $POLY_COUNT Polymarket BTC15M markets"

if [ "$POLY_COUNT" -gt "0" ]; then
    FIRST_TOKEN=$(echo "$POLY_RESPONSE" | jq -r '.[0].tokens[0].token_id' 2>/dev/null)
    echo "   First token ID: $FIRST_TOKEN"
    
    if [ -n "$FIRST_TOKEN" ] && [ "$FIRST_TOKEN" != "null" ]; then
        echo ""
        echo "📖 Testing Polymarket Orderbook for token $FIRST_TOKEN..."
        echo "-----------------------------------------------------------"
        
        BOOK_RESPONSE=$(curl -s "https://clob.polymarket.com/book?token_id=$FIRST_TOKEN")
        
        ASK_COUNT=$(echo "$BOOK_RESPONSE" | jq '.asks | length' 2>/dev/null || echo "0")
        BID_COUNT=$(echo "$BOOK_RESPONSE" | jq '.bids | length' 2>/dev/null || echo "0")
        
        echo "✅ Orderbook: $ASK_COUNT asks, $BID_COUNT bids"
        
        if [ "$ASK_COUNT" -gt "0" ]; then
            BEST_ASK=$(echo "$BOOK_RESPONSE" | jq -r '.asks[0]' 2>/dev/null)
            echo "   Best Ask: $BEST_ASK"
        fi
        
        if [ "$BID_COUNT" -gt "0" ]; then
            BEST_BID=$(echo "$BOOK_RESPONSE" | jq -r '.bids[0]' 2>/dev/null)
            echo "   Best Bid: $BEST_BID"
        fi
    fi
fi

echo ""
echo "✅ Data feed test complete!"
