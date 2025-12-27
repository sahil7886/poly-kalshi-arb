#!/usr/bin/env python3
"""
One-time setup script to enable selling on Polymarket.
Sets CTF approval for the Exchange contract to manage your tokens.

Run this ONCE before using the bot for live trading.
Requires: pip install eth-account web3 python-dotenv
"""

import os
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

# Polymarket Polygon Mainnet addresses
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Token Framework
EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # Polymarket Exchange

# ERC-1155 setApprovalForAll ABI
CTF_ABI = [
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"}
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "operator", "type": "address"}
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function"
    }
]

def main():
    load_dotenv()
    
    private_key = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER")
    rpc_url = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    
    if not private_key:
        print("❌ POLY_PRIVATE_KEY not set in .env")
        return
    
    # Derive wallet address from private key
    account = Account.from_key(private_key)
    signer_address = account.address
    
    print(f"🔑 Signer: {signer_address}")
    print(f"📦 Funder: {funder}")
    print(f"🌐 RPC: {rpc_url}")
    print()
    
    # Connect to Polygon
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        print("❌ Failed to connect to Polygon RPC")
        return
    
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
    
    # Check if already approved
    # Note: We need to check approval for the FUNDER, not the signer
    # If funder is a proxy wallet, the signer may need to approve on behalf of funder
    check_address = funder if funder else signer_address
    is_approved = ctf.functions.isApprovedForAll(
        Web3.to_checksum_address(check_address),
        Web3.to_checksum_address(EXCHANGE_ADDRESS)
    ).call()
    
    if is_approved:
        print(f"✅ Already approved! Exchange can manage tokens for {check_address}")
        return
    
    print(f"⚠️  Not approved. Setting approval for {check_address}...")
    print()
    
    # Build transaction
    # Note: If funder is a proxy wallet, this may not work - needs proxy signature
    tx = ctf.functions.setApprovalForAll(
        Web3.to_checksum_address(EXCHANGE_ADDRESS),
        True
    ).build_transaction({
        "from": signer_address,
        "nonce": w3.eth.get_transaction_count(signer_address),
        "gas": 100000,
        "gasPrice": w3.eth.gas_price,
        "chainId": 137
    })
    
    # Check MATIC balance
    balance = w3.eth.get_balance(signer_address)
    gas_cost = tx["gas"] * tx["gasPrice"]
    print(f"💰 MATIC balance: {w3.from_wei(balance, 'ether'):.4f}")
    print(f"⛽ Estimated gas cost: {w3.from_wei(gas_cost, 'ether'):.6f} MATIC")
    
    if balance < gas_cost:
        print(f"❌ Insufficient MATIC! Need ~{w3.from_wei(gas_cost, 'ether'):.6f} MATIC")
        return
    
    # Confirm before sending
    confirm = input("\n🚀 Send transaction? (y/N): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return
    
    # Sign and send
    signed = w3.eth.account.sign_transaction(tx, private_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"📤 Tx sent: {tx_hash.hex()}")
    
    # Wait for confirmation
    print("⏳ Waiting for confirmation...")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    
    if receipt["status"] == 1:
        print(f"✅ Success! Tx: https://polygonscan.com/tx/{tx_hash.hex()}")
    else:
        print(f"❌ Transaction failed! Check: https://polygonscan.com/tx/{tx_hash.hex()}")

if __name__ == "__main__":
    main()
