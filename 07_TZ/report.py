import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://mikitaslesarchuk:NewStrongPassword123@localhost:5432/tvl_db")
VAULT = "0x8ECC0B419dfe3AE197BC96f2a03636b5E1BE91db"
from_ts = "2026-01-10 00:00:00+00"
to_ts   = "2026-01-17 23:59:59+00"

engine = sa.create_engine(DB_URL)

q = """
SELECT mt.code, COALESCE(m.block_timestamp, m.collected_at) AS ts, m.value_numeric::numeric AS val
FROM metrics m
JOIN metric_types mt ON mt.id = m.metric_type_id
JOIN vaults v        ON v.id  = m.vault_id
WHERE v.address_proxy = :vault
  AND COALESCE(m.block_timestamp, m.collected_at) BETWEEN :from_ts AND :to_ts
ORDER BY ts
"""

df = pd.read_sql(sa.text(q), engine, params={"vault": VAULT, "from_ts": from_ts, "to_ts": to_ts})

def series(code: str) -> pd.DataFrame:
    s = (
        df[df["code"] == code][["ts", "val"]]
        .copy()
        .drop_duplicates(subset=["ts"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    s.rename(columns={"val": code}, inplace=True)
    return s

sp = series("SHARE_PRICE")
tvl = series("TVL_ASSET")

if sp.empty:
    raise SystemExit("Нет данных PPS за период")

sp["prev"] = sp["SHARE_PRICE"].shift(1)
sp["log_ret"] = (sp["SHARE_PRICE"] / sp["prev"]).apply(
    lambda x: None if pd.isna(x) or x == 0 else np.log(x)
)

sp_open  = sp["SHARE_PRICE"].iloc[0]
sp_close = sp["SHARE_PRICE"].iloc[-1]
period_return = sp_close / sp_open - 1.0

sp["run_max"]  = sp["SHARE_PRICE"].cummax()
sp["drawdown"] = sp["SHARE_PRICE"] / sp["run_max"] - 1.0
max_drawdown   = sp["drawdown"].min()

volatility_logret = sp["log_ret"].dropna().std()

tvl_change = None
if not tvl.empty:
    tvl_open, tvl_close = tvl["TVL_ASSET"].iloc[0], tvl["TVL_ASSET"].iloc[-1]
    tvl_change = (tvl_close / tvl_open - 1.0) if tvl_open else None

print("=== REPORT ===")
print(f"Period: {from_ts} .. {to_ts} UTC")
print(f"Points: {sp.shape[0]}")
print(f"Return: {period_return*100:.4f}%")
print(f"MaxDD:  {max_drawdown*100:.4f}%")
print(f"Sigma:  {volatility_logret*100:.4f}%")
if tvl_change is not None:
    print(f"ΔTVL:   {tvl_change*100:.4f}%")
