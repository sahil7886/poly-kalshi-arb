#!/usr/bin/env python3
"""Show the wallet address derived from POLY_PRIVATE_KEY"""

import os
from eth_account import Account
from dotenv import load_dotenv

load_dotenv()

private_key = os.getenv("POLY_PRIVATE_KEY")
if not private_key:
    print("ERROR: POLY_PRIVATE_KEY not set in .env")
    exit(1)

account = Account.from_key(private_key)
print(f"\n{'='*60}")
print("POLYMARKET WALLET INFO")
print(f"{'='*60}")
print(f"Signer Address (needs MATIC): {account.address}")
print(f"Funder Address (POLY_FUNDER): {os.getenv('POLY_FUNDER', 'not set')}")
print(f"{'='*60}")
print("\nTo fund MATIC on Polygon Mainnet:")
print("1. Buy MATIC on Coinbase/Binance -> withdraw to Polygon network")
print("2. Bridge from Ethereum: https://wallet.polygon.technology/")
print("3. Use a swap service like https://jumper.exchange/")
print(f"\nSend 0.5-1 MATIC to: {account.address}")
print(f"{'='*60}\n")
