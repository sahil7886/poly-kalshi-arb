#!/usr/bin/env python3
"""
Fetch Kalshi order history to see what actually happened
"""
import os
import time
import base64
import hashlib
import hmac
from dotenv import load_dotenv
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes

load_dotenv()

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem")
API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

def sign_message(message: str, private_key_path: str) -> str:
    """Sign message using RSA private key"""
    with open(private_key_path, 'rb') as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    
    signature = private_key.sign(
        message.encode('utf-8'),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')

def get_orders():
    """Fetch order history from Kalshi"""
    timestamp_ms = int(time.time() * 1000)
    path = "/portfolio/orders"
    full_path = f"/trade-api/v2{path}"
    message = f"{timestamp_ms}GET{full_path}"
    
    signature = sign_message(message, PRIVATE_KEY_PATH)
    
    headers = {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
    }
    
    url = f"{API_BASE}{path}"
    print(f"Fetching: {url}")
    
    resp = requests.get(url, headers=headers)
    print(f"Status: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"\nFound {len(data.get('orders', []))} orders")
        return data
    else:
        print(f"Error: {resp.text}")
        return None

if __name__ == "__main__":
    result = get_orders()
    if result:
        import json
        print("\n" + "="*80)
        print("ORDER HISTORY:")
        print("="*80)
        
        orders = result.get('orders', [])
        # Filter for BTC 15m market orders
        btc_orders = [o for o in orders if 'BTC15M' in o.get('ticker', '')]
        
        for order in sorted(btc_orders, key=lambda x: x.get('created_time', ''), reverse=True)[:10]:
            print(f"\nOrder ID: {order.get('order_id')}")
            print(f"Ticker: {order.get('ticker')}")
            print(f"Action: {order.get('action')} | Side: {order.get('side')}")
            print(f"Status: {order.get('status')}")
            print(f"Created: {order.get('created_time')}")
            print(f"Filled: {order.get('taker_fill_count', 0) + order.get('maker_fill_count', 0)}")
            print(f"Place Count: {order.get('place_count')}")
            print(f"Remaining: {order.get('remaining_count')}")
            print(f"Cost: ${(order.get('taker_fill_cost', 0) + order.get('maker_fill_cost', 0)) / 100:.2f}")
            print("-" * 80)
