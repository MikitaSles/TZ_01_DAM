from web3 import Web3

RPC_URL = ""
VAULT_ADDRESS = ""


ERC4626_ABI = [
   {
       "name": "asset",
       "inputs": [],
       "outputs": [{"type": "address"}],
       "stateMutability": "view",
       "type": "function",
   },
   {
       "name": "totalAssets",
       "inputs": [],
       "outputs": [{"type": "uint256"}],
       "stateMutability": "view",
       "type": "function",
   },


]
#минимальный abi под erc20 токен
ERC20_ABI = [
   {
       "name": "decimals",
       "inputs": [],
       "outputs": [{"type": "uint8"}],
       "stateMutability": "view",
       "type": "function",
   },
   {
       "name": "symbol",
       "inputs": [],
       "outputs": [{"type": "string"}],
       "stateMutability": "view",
       "type": "function",
   },
]

w3 = Web3(Web3.HTTPProvider(RPC_URL))

vault = w3.eth.contract(
   address=Web3.to_checksum_address(VAULT_ADDRESS),
   abi=ERC4626_ABI,
)

asset_addr = vault.functions.asset().call()
asset = w3.eth.contract(
   address=Web3.to_checksum_address(asset_addr),
   abi=ERC20_ABI
)

symbol = asset.functions.symbol().call()
decimals = asset.functions.decimals().call()

raw_total_assets = vault.functions.totalAssets().call()
tvl = raw_total_assets / (10 ** decimals)

print("===== TVL для волта =====")
print("Vault:", VAULT_ADDRESS)
print("Base asset:", symbol)
print("TVL (raw):", raw_total_assets)
print(f"TVL (человекочитаемо): {tvl:,.6f} {symbol}")
print("номер блока:", w3.eth.block_number)

