#!/usr/bin/env python3
"""
Approve Polymarket CLOB exchange to spend USDC from your wallet.
This is required ONCE before trading on Polymarket.

Run: python3 scripts/approve_usdc.py
Requires: pip install eth-account web3 python-dotenv
"""

import os
from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

# Polygon Mainnet addresses
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC on Polygon
EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # Polymarket CLOB Exchange
EXCHANGE_NEG_RISK = "0xC5d563A36AE78145C45a50134d48A1215220f80a"  # Neg-risk exchange

# ERC20 approve ABI
ERC20_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
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
    
    if not funder:
        print("❌ POLY_FUNDER not set in .env")
        return
    
    # Derive signer address
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
    
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI)
    
    # Check USDC balance
    funder_checksum = Web3.to_checksum_address(funder)
    balance = usdc.functions.balanceOf(funder_checksum).call()
    balance_usdc = balance / 1e6  # USDC has 6 decimals
    print(f"💵 USDC Balance: ${balance_usdc:.2f}")
    
    if balance == 0:
        print("⚠️  Warning: No USDC in funder wallet! Deposit USDC before trading.")
        print()
    
    # Check current allowances for both exchanges
    for name, exchange in [("Standard", EXCHANGE_ADDRESS), ("Neg-Risk", EXCHANGE_NEG_RISK)]:
        exchange_checksum = Web3.to_checksum_address(exchange)
        allowance = usdc.functions.allowance(funder_checksum, exchange_checksum).call()
        allowance_usdc = allowance / 1e6
        
        print(f"📊 {name} Exchange ({exchange}):")
        print(f"   Current allowance: ${allowance_usdc:.2f}")
        
        if allowance >= balance and balance > 0:
            print(f"   ✅ Already approved (sufficient allowance)")
        elif allowance > 0:
            print(f"   ⚠️  Partial allowance (may need more for large trades)")
        else:
            print(f"   ❌ No allowance - needs approval")
        print()
    
    # Ask which exchange to approve
    print("Which exchange do you want to approve?")
    print("1. Standard Exchange (most markets)")
    print("2. Neg-Risk Exchange (15-minute crypto markets)")
    print("3. Both (recommended)")
    choice = input("Enter choice (1/2/3): ").strip()
    
    exchanges_to_approve = []
    if choice == "1":
        exchanges_to_approve = [("Standard", EXCHANGE_ADDRESS)]
    elif choice == "2":
        exchanges_to_approve = [("Neg-Risk", EXCHANGE_NEG_RISK)]
    elif choice == "3":
        exchanges_to_approve = [("Standard", EXCHANGE_ADDRESS), ("Neg-Risk", EXCHANGE_NEG_RISK)]
    else:
        print("Invalid choice. Exiting.")
        return
    
    # Approve unlimited (max uint256) for convenience
    # This is standard practice - you approve once and don't need to re-approve
    max_uint256 = 2**256 - 1
    
    for name, exchange in exchanges_to_approve:
        print(f"\n🔓 Approving {name} Exchange to spend USDC...")
        exchange_checksum = Web3.to_checksum_address(exchange)
        
        # Check MATIC balance for gas
        signer_checksum = Web3.to_checksum_address(signer_address)
        matic_balance = w3.eth.get_balance(signer_checksum)
        gas_price = w3.eth.gas_price
        gas_limit = 100000
        gas_cost = gas_limit * gas_price
        
        print(f"💰 MATIC balance: {w3.from_wei(matic_balance, 'ether'):.4f}")
        print(f"⛽ Estimated gas cost: {w3.from_wei(gas_cost, 'ether'):.6f} MATIC")
        
        if matic_balance < gas_cost:
            print(f"❌ Insufficient MATIC! Need ~{w3.from_wei(gas_cost, 'ether'):.6f} MATIC for gas")
            print(f"   Deposit MATIC to {signer_address}")
            continue
        
        # Build approve transaction
        # NOTE: The transaction is FROM the funder address, but SIGNED by the signer
        # This only works if funder == signer OR if funder is a proxy wallet
        tx = usdc.functions.approve(
            exchange_checksum,
            max_uint256
        ).build_transaction({
            "from": signer_checksum,
            "nonce": w3.eth.get_transaction_count(signer_checksum),
            "gas": gas_limit,
            "gasPrice": gas_price,
            "chainId": 137
        })
        
        # Confirm
        confirm = input(f"\n🚀 Approve {name} Exchange? (y/N): ").strip().lower()
        if confirm != "y":
            print("Skipped.")
            continue
        
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
    
    print("\n✨ Done! You can now trade on Polymarket.")

if __name__ == "__main__":
    main()
