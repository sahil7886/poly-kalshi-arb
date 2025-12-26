import requests
import json
from datetime import datetime

SERIES = "KXBTC15M"
URL = f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={SERIES}&limit=20&status=open"

print(f"Current UTC time: {datetime.utcnow()}")
print(f"Fetching {URL}...")
resp = requests.get(URL)
try:
    data = resp.json()
    markets = data.get("markets", [])
    print(f"Found {len(markets)} markets.")
    for m in markets:
        print(f"\nTicker: {m.get('ticker')}")
        print(f"Status: {m.get('status')}")
        print(f"Open: {m.get('open_time')} | Close: {m.get('close_time')}")
        print(f"Yes Ask: {m.get('yes_ask')} | No Ask: {m.get('no_ask')}")
except Exception as e:
    print(f"Error: {e}")
    print(resp.text)
