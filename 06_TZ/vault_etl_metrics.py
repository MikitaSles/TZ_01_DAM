import os
import sys
import logging
import argparse
import datetime as dt
from decimal import Decimal, getcontext
from dotenv import load_dotenv
from web3 import Web3
from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, Numeric, String, TIMESTAMP,
    UniqueConstraint
)
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.dialects.postgresql import insert as pg_insert

getcontext().prec = 50
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("vault-etl")

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
    {"name": "symbol", "inputs": [], "outputs": [{"type": "string"}],
     "stateMutability": "view", "type": "function"},
]

class Base(DeclarativeBase):
    pass

class Vault(Base):
    __tablename__ = "vaults"
    id = Column(Integer, primary_key=True)
    address_proxy = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)

class MetricType(Base):
    __tablename__ = "metric_types"
    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)  # TVL, SHARE_PRICE
    name = Column(String, nullable=False)

class Metric(Base):
    __tablename__ = "metrics"
    id = Column(BigInteger, primary_key=True)
    vault_id = Column(Integer, nullable=False)
    metric_type_id = Column(Integer, nullable=False)
    block_number = Column(BigInteger, nullable=True)
    block_timestamp = Column(TIMESTAMP, nullable=True)
    collected_at = Column(TIMESTAMP, nullable=False)
    value_numeric = Column(Numeric(38, 24), nullable=False)
    source = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint("vault_id", "metric_type_id", "block_number",
                         name="uq_metrics_vault_metric_block"),
    )

def ensure_schema(engine):
    Base.metadata.create_all(bind=engine)

def ensure_metric_types(sess):
    data = [
        {"code": "TVL_ASSET", "name": "TVL в базовом активе"},
        {"code": "SHARE_PRICE", "name": "Цена доли (PPS)"},
    ]
    for row in data:
        stmt = pg_insert(MetricType).values(**row).on_conflict_do_nothing(index_elements=['code'])
        sess.execute(stmt)
    sess.commit()

def get_metric_type_id(sess, code: str) -> int:
    rec = sess.query(MetricType).filter(MetricType.code == code).one()
    return rec.id

def ensure_vaults(sess, vault_addrs):
    for addr in vault_addrs:
        addr = Web3.to_checksum_address(addr)
        stmt = pg_insert(Vault).values(address_proxy=addr).on_conflict_do_nothing(index_elements=['address_proxy'])
        sess.execute(stmt)
    sess.commit()

def get_vault_id(sess, address_proxy: str) -> int:
    rec = sess.query(Vault).filter(Vault.address_proxy == Web3.to_checksum_address(address_proxy)).one()
    return rec.id

def upsert_metric(sess, vault_id: int, metric_type_id: int, block_number: int,
                  block_timestamp: dt.datetime, collected_at: dt.datetime,
                  value: Decimal, source: str):
    stmt = pg_insert(Metric).values(
        vault_id=vault_id,
        metric_type_id=metric_type_id,
        block_number=block_number,
        block_timestamp=block_timestamp,
        collected_at=collected_at,
        value_numeric=value,
        source=source
    ).on_conflict_do_nothing(
        index_elements=["vault_id", "metric_type_id", "block_number"]
    )
    sess.execute(stmt)

def get_w3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise RuntimeError("Не удалось подключиться к RPC")
    return w3

def erc4626_contract(w3: Web3, vault_address: str):
    return w3.eth.contract(address=Web3.to_checksum_address(vault_address), abi=ERC4626_ABI)

def erc20_contract(w3: Web3, token_address: str):
    return w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)

def read_asset_decimals(w3: Web3, vault_addr: str) -> int:
    vault = erc4626_contract(w3, vault_addr)
    token_addr = vault.functions.asset().call()
    token = erc20_contract(w3, token_addr)
    decimals = token.functions.decimals().call()
    return int(decimals)

def read_total_assets(w3: Web3, vault_addr: str, block_number=None) -> int:
    vault = erc4626_contract(w3, vault_addr)
    if block_number is None:
        return int(vault.functions.totalAssets().call())
    return int(vault.functions.totalAssets().call(block_identifier=block_number))

def read_total_supply(w3: Web3, vault_addr: str, block_number=None) -> int:
    vault = erc4626_contract(w3, vault_addr)
    if block_number is None:
        return int(vault.functions.totalSupply().call())
    return int(vault.functions.totalSupply().call(block_identifier=block_number))

def get_block_timestamp(w3: Web3, block_number: int) -> dt.datetime:
    blk = w3.eth.get_block(block_number)
    ts = int(blk["timestamp"])
    return dt.datetime.fromtimestamp(ts, dt.UTC)

def find_block_by_time(w3: Web3, target_ts: int) -> int:
    hi = w3.eth.block_number
    lo = 0
    lo_ts = int(w3.eth.get_block(lo)["timestamp"])
    if target_ts <= lo_ts:
        return lo

    while lo + 1 < hi:
        mid = (lo + hi) // 2
        mid_ts = int(w3.eth.get_block(mid)["timestamp"])
        if mid_ts <= target_ts:
            lo = mid
        else:
            hi = mid
    return lo

def collect_at_block(w3: Web3, sess, vault_addr: str, block_number: int, source: str):
    vault_id = get_vault_id(sess, vault_addr)
    mt_tvl = get_metric_type_id(sess, "TVL_ASSET")
    mt_pps = get_metric_type_id(sess, "SHARE_PRICE")
    decimals = read_asset_decimals(w3, vault_addr)
    ta_raw = read_total_assets(w3, vault_addr, block_number=block_number)
    ts_raw = read_total_supply(w3, vault_addr, block_number=block_number)
    scale = Decimal(10) ** Decimal(decimals)
    tvl_asset = (Decimal(ta_raw) / scale).quantize(Decimal("0.000000"))
    if ts_raw > 0:
        share_price = (Decimal(ta_raw) / Decimal(ts_raw)).quantize(Decimal("0.000000000000000000"))
    else:
        share_price = Decimal(0)

    block_ts = get_block_timestamp(w3, block_number)
    collected_at = dt.datetime.now(dt.UTC)

    if tvl_asset < 0 or share_price < 0:
        log.warning("Отрицательные значения: пропуск записи")
        return

    upsert_metric(sess, vault_id, mt_tvl, block_number, block_ts, collected_at, tvl_asset, source)
    upsert_metric(sess, vault_id, mt_pps, block_number, block_ts, collected_at, share_price, source)
    sess.commit()

    log.info(f"[OK] {vault_addr} block={block_number} TVL={tvl_asset} PPS={share_price}")

def run_incremental(w3: Web3, sess, vault_addrs):
    latest = w3.eth.block_number
    for addr in vault_addrs:
        try:
            collect_at_block(w3, sess, addr, latest, source="incremental:latest")
        except Exception as e:
            log.exception(f"[ERR] {addr}: {e}")

def parse_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "")).replace(tzinfo=None)

def run_backfill(w3: Web3, sess, vault_addrs, start_iso: str, end_iso: str, step_sec: int):
    start = parse_iso(start_iso)
    end = parse_iso(end_iso)
    if end <= start:
        raise ValueError("end_iso должен быть больше start_iso")
    step = dt.timedelta(seconds=step_sec)

    t = start
    while t <= end:
        target_ts = int(t.timestamp())
        block = find_block_by_time(w3, target_ts)
        log.info(f"[backfill] {t.isoformat()} UTC -> block {block}")
        for addr in vault_addrs:
            try:
                collect_at_block(w3, sess, addr, block, source="backfill")
            except Exception as e:
                log.exception(f"[ERR] backfill {addr} at {t}: {e}")
        t += step

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Vault Metrics ETL (ERC-4626)")
    sub = parser.add_subparsers(dest="mode", required=True)
    p_inc = sub.add_parser("incremental", help="Разовый сбор на latest-блоке (под cron)")
    p_bf = sub.add_parser("backfill", help="Сбор за период по времени (UTC)")
    p_bf.add_argument("--start-iso", required=True, help="Начало периода, ISO (например, 2025-12-01T00:00:00)")
    p_bf.add_argument("--end-iso", required=True, help="Конец периода, ISO (например, 2025-12-07T23:59:59)")
    p_bf.add_argument("--step-sec", type=int, default=300, help="Шаг в секундах (по умолчанию 300)")
    args = parser.parse_args()
    rpc_url = os.getenv("RPC_URL")
    db_url = os.getenv("DB_URL")
    vault_addrs_env = os.getenv("VAULT_ADDRESSES", "")

    if not rpc_url or not db_url or not vault_addrs_env:
        log.error("Не заданы RPC_URL, DB_URL или VAULT_ADDRESSES в .env")
        sys.exit(1)

    vault_addrs = [a.strip() for a in vault_addrs_env.split(",") if a.strip()]
    engine = create_engine(db_url, echo=False, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    ensure_schema(engine)

    with Session() as sess:
        ensure_metric_types(sess)
        ensure_vaults(sess, vault_addrs)
        w3 = get_w3(rpc_url)

        if args.mode == "incremental":
            run_incremental(w3, sess, vault_addrs)
        elif args.mode == "backfill":
            run_backfill(w3, sess, vault_addrs, args.start_iso, args.end_iso, args.step_sec)
        else:
            parser.print_help()







