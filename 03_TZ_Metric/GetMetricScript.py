from web3 import Web3
from db import SessionLocal
from models import TVLMetrics

RPC_URL = ""
VAULT_ADDRESS = "0x8ECC0B419dfe3AE197BC96f2a03636b5E1BE91db"

ERC4626_ABI = [
   {"name": "asset", "inputs": [], "outputs": [{"type": "address"}],
    "stateMutability": "view", "type": "function"},
   {"name": "totalAssets", "inputs": [], "outputs": [{"type": "uint256"}],
    "stateMutability": "view", "type": "function"},
   {"name": "totalSupply", "inputs": [], "outputs": [{"type": "uint256"}],
    "stateMutability": "view", "type": "function"},
]
ERC20_ABI = [
   {"name": "decimals", "inputs": [], "outputs": [{"type": "uint8"}],
    "stateMutability": "view", "type": "function"},
]


def collect_metrics():
   w3 = Web3(Web3.HTTPProvider(RPC_URL))

   vault = w3.eth.contract(
       address=Web3.to_checksum_address(VAULT_ADDRESS),
       abi=ERC4626_ABI
   )
   asset_addr = vault.functions.asset().call()
   asset = w3.eth.contract(
       address=Web3.to_checksum_address(asset_addr),
       abi=ERC20_ABI
   )

   decimals = asset.functions.decimals().call()
   total_assets_raw = vault.functions.totalAssets().call()
   total_supply_raw = vault.functions.totalSupply().call()
   block = w3.eth.block_number

   tvl = total_assets_raw / (10 ** decimals)
   share_price = (
       total_assets_raw / total_supply_raw if total_supply_raw > 0 else 0
   )

   print(f"TVL: {tvl}")
   print(f"Share Price: {share_price}")
   print(f"Block: {block}")

   db = SessionLocal()
   record = TVLMetrics(
       tvl=tvl,
       share_price=share_price,
       block_number=block
   )
   db.add(record)
   db.commit()
   db.close()


if __name__ == "__main__":
    collect_metrics()
