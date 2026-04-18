from __future__ import annotations

import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from statistics import median
from typing import Any

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


MOEX_ISS_BASE = os.getenv("MOEX_ISS_BASE", "https://iss.moex.com/iss").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL")
TOP_STOCKS = [
    ("SBER", "Сбербанк ао | SBER"),
    ("GAZP", "Газпром | GAZP"),
    ("LKOH", "Лукойл | LKOH"),
    ("ROSN", "Роснефть | ROSN"),
    ("GMKN", "Норникель | GMKN"),
    ("NVTK", "Новатэк | NVTK"),
    ("TATN", "Татнефть ао | TATN"),
    ("MGNT", "Магнит | MGNT"),
    ("MOEX", "Московская биржа | MOEX"),
    ("YDEX", "Яндекс | YDEX"),
]
PERPETUAL_OVERRIDES = {
    "IMOEXF": "Вечный фьючерс на индекс Мосбиржи | IMOEXF",
    "SBERF": "Вечный фьючерс на Сбербанк | SBERF",
    "GAZPF": "Вечный фьючерс на Газпром | GAZPF",
    "GLDRUBF": "Вечный фьючерс на золото | GLDRUBF",
    "SLVRUBF": "Вечный фьючерс на серебро | SLVRUBF",
    "USDRUBF": "Вечный фьючерс USD/RUB | USDRUBF",
    "EURRUBF": "Вечный фьючерс EUR/RUB | EURRUBF",
    "CNYRUBF": "Вечный фьючерс CNY/RUB | CNYRUBF",
    "RGBIF": "Вечный фьючерс RGBI | RGBIF",
}
_SCHEMA_READY = False


class AnalyzeRequest(BaseModel):
    ticker1: str = Field(min_length=1)
    ticker2: str = Field(min_length=1)
    start_date: date


@dataclass
class InstrumentRecord:
    instrument_id: str
    secid: str
    engine: str
    market: str
    boardid: str
    shortname: str
    display_name: str
    assetcode: str | None
    start_date: date | None
    end_date: date | None
    expiration_date: date | None
    perpetual: bool
    category: str


app = FastAPI(title="IMOEX Pair Spread Lab", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def require_database_url() -> str:
    if not DATABASE_URL:
        raise HTTPException(
            status_code=500,
            detail="DATABASE_URL is not configured. Add it in Vercel project settings or local .env.",
        )
    return DATABASE_URL


def get_connection() -> psycopg.Connection:
    return psycopg.connect(require_database_url(), autocommit=True)


def ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists instruments (
                    instrument_id text primary key,
                    secid text not null,
                    shortname text,
                    display_name text,
                    assetcode text,
                    engine text not null,
                    market text not null,
                    boardid text not null,
                    start_date date,
                    end_date date,
                    expiration_date date,
                    perpetual boolean not null default false,
                    category text not null default 'other',
                    updated_at timestamptz not null default now()
                )
                """
            )
            cur.execute(
                """
                create table if not exists candles_10m (
                    instrument_id text not null references instruments(instrument_id) on delete cascade,
                    begin_ts timestamp not null,
                    end_ts timestamp not null,
                    open double precision,
                    close double precision,
                    high double precision,
                    low double precision,
                    volume double precision,
                    value bigint,
                    primary key (instrument_id, begin_ts)
                )
                """
            )
            cur.execute(
                "create index if not exists idx_candles_10m_lookup on candles_10m (instrument_id, begin_ts desc)"
            )

    _SCHEMA_READY = True


def instrument_id(engine: str, market: str, boardid: str, secid: str) -> str:
    return f"{engine}:{market}:{boardid}:{secid.upper()}"


def normalize_ticker(raw_ticker: str) -> str:
    return raw_ticker.strip().split("|")[0].upper()


async def moex_get_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    timeout = httpx.Timeout(40.0, connect=10.0)
    async with httpx.AsyncClient(base_url=MOEX_ISS_BASE, timeout=timeout) as client:
        response = await client.get(path, params=params)
        response.raise_for_status()
        return response.json()


def rows_to_dicts(payload: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
    block = payload.get(block_name, {})
    columns = block.get("columns", [])
    return [dict(zip(columns, row)) for row in block.get("data", [])]


def choose_primary_board(boards: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [
        board
        for board in boards
        if board.get("is_primary") == 1 and board.get("engine") in {"futures", "stock"} and board.get("history_from")
    ]
    if candidates:
        return candidates[0]

    candidates = [board for board in boards if board.get("is_traded") == 1 and board.get("history_from")]
    if candidates:
        return candidates[0]

    if boards:
        return boards[0]

    raise HTTPException(status_code=404, detail="Ticker found but no board with history is available")


def first_description_value(descriptions: list[dict[str, Any]], name: str) -> Any:
    for row in descriptions:
        if row.get("name") == name:
            return row.get("value")
    return None


def parse_date(raw: Any) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def parse_datetime(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


def build_display_name(secid: str, shortname: str | None, description_name: str | None, perpetual: bool) -> str:
    if secid in PERPETUAL_OVERRIDES:
        return PERPETUAL_OVERRIDES[secid]

    if perpetual and description_name:
        return f"{description_name} | {secid}"

    if shortname and shortname != secid:
        return f"{shortname} | {secid}"

    if description_name:
        return f"{description_name} | {secid}"

    return secid


async def resolve_instrument(secid: str) -> InstrumentRecord:
    secid = normalize_ticker(secid)
    payload = await moex_get_json(f"/securities/{secid}.json")
    descriptions = rows_to_dicts(payload, "description")
    boards = rows_to_dicts(payload, "boards")

    if not descriptions and not boards:
        raise HTTPException(status_code=404, detail=f"Ticker {secid} not found on MOEX")

    board = choose_primary_board(boards)
    engine = str(board.get("engine"))
    market = str(board.get("market"))
    boardid = str(board.get("boardid"))
    shortname = str(first_description_value(descriptions, "SHORTNAME") or secid)
    contract_name = first_description_value(descriptions, "CONTRACTNAME") or first_description_value(
        descriptions, "NAME"
    )
    perpetual = str(first_description_value(descriptions, "PERPETUAL_FUTURES") or "0") == "1"
    category = "futures" if engine == "futures" else "shares" if market == "shares" else "other"

    return InstrumentRecord(
        instrument_id=instrument_id(engine, market, boardid, secid),
        secid=secid,
        engine=engine,
        market=market,
        boardid=boardid,
        shortname=shortname,
        display_name=build_display_name(secid, shortname, contract_name, perpetual),
        assetcode=first_description_value(descriptions, "ASSETCODE"),
        start_date=parse_date(board.get("history_from") or first_description_value(descriptions, "FRSTTRADE")),
        end_date=parse_date(board.get("history_till") or first_description_value(descriptions, "LSTTRADE")),
        expiration_date=parse_date(first_description_value(descriptions, "LSTDELDATE")),
        perpetual=perpetual,
        category=category,
    )


def upsert_instrument(record: InstrumentRecord) -> None:
    ensure_schema()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into instruments (
                    instrument_id, secid, shortname, display_name, assetcode, engine, market, boardid,
                    start_date, end_date, expiration_date, perpetual, category, updated_at
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (instrument_id) do update set
                    shortname = excluded.shortname,
                    display_name = excluded.display_name,
                    assetcode = excluded.assetcode,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    expiration_date = excluded.expiration_date,
                    perpetual = excluded.perpetual,
                    category = excluded.category,
                    updated_at = now()
                """,
                (
                    record.instrument_id,
                    record.secid,
                    record.shortname,
                    record.display_name,
                    record.assetcode,
                    record.engine,
                    record.market,
                    record.boardid,
                    record.start_date,
                    record.end_date,
                    record.expiration_date,
                    record.perpetual,
                    record.category,
                ),
            )


async def sync_candles(record: InstrumentRecord, start_date: date, end_date: date) -> None:
    ensure_schema()
    upsert_instrument(record)

    rows: list[tuple[Any, ...]] = []
    offset = 0
    while True:
        payload = await moex_get_json(
            f"/engines/{record.engine}/markets/{record.market}/boards/{record.boardid}/securities/{record.secid}/candles.json",
            params={
                "from": start_date.isoformat(),
                "till": end_date.isoformat(),
                "interval": 10,
                "start": offset,
            },
        )
        candle_rows = rows_to_dicts(payload, "candles")
        if not candle_rows:
            break

        for row in candle_rows:
            rows.append(
                (
                    record.instrument_id,
                    parse_datetime(row["begin"]),
                    parse_datetime(row["end"]),
                    row.get("open"),
                    row.get("close"),
                    row.get("high"),
                    row.get("low"),
                    row.get("volume"),
                    row.get("value"),
                )
            )

        offset += len(candle_rows)

    if not rows:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into candles_10m (
                    instrument_id, begin_ts, end_ts, open, close, high, low, volume, value
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (instrument_id, begin_ts) do update set
                    end_ts = excluded.end_ts,
                    open = excluded.open,
                    close = excluded.close,
                    high = excluded.high,
                    low = excluded.low,
                    volume = excluded.volume,
                    value = excluded.value
                """,
                rows,
            )


def fetch_series(record: InstrumentRecord, start_date: date, end_date: date) -> list[dict[str, Any]]:
    ensure_schema()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select begin_ts, close, open, high, low, volume
                from candles_10m
                where instrument_id = %s
                  and begin_ts >= %s::date
                  and begin_ts < (%s::date + interval '1 day')
                order by begin_ts asc
                """,
                (record.instrument_id, start_date, end_date),
            )
            rows = cur.fetchall()

    return [
        {
            "ts": row[0].isoformat(),
            "close": float(row[1]) if row[1] is not None else None,
            "open": float(row[2]) if row[2] is not None else None,
            "high": float(row[3]) if row[3] is not None else None,
            "low": float(row[4]) if row[4] is not None else None,
            "volume": float(row[5]) if row[5] is not None else None,
        }
        for row in rows
    ]


def align_spread(series1: list[dict[str, Any]], series2: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lookup2 = {row["ts"]: row for row in series2}
    aligned: list[dict[str, Any]] = []
    for row1 in series1:
        row2 = lookup2.get(row1["ts"])
        if not row2:
            continue
        close1 = row1["close"]
        close2 = row2["close"]
        if close1 is None or close2 is None:
            continue
        avg_price = (close1 + close2) / 2
        spread = close1 - close2
        spread_pct = (spread / avg_price) * 100 if avg_price else None
        aligned.append(
            {
                "ts": row1["ts"],
                "close1": close1,
                "close2": close2,
                "spread": spread,
                "spread_pct": spread_pct,
            }
        )
    return aligned


def stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def monthly_statistics(spread_series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in spread_series:
        month_key = row["ts"][:7]
        groups[month_key].append(row)

    stats: list[dict[str, Any]] = []
    for month_key in sorted(groups):
        values = [row["spread"] for row in groups[month_key]]
        stats.append(
            {
                "month": month_key,
                "month_date": f"{month_key}-01",
                "observations": len(values),
                "min_spread": min(values),
                "median_spread": median(values),
                "max_spread": max(values),
                "std_spread": stddev(values),
            }
        )
    return stats


def build_seasonality(monthly_stats_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_year: dict[str, dict[str, float | None]] = defaultdict(dict)
    month_order: list[str] = []
    for row in monthly_stats_rows:
        year, month = row["month"].split("-")
        by_year[year][month] = row["median_spread"]
        if month not in month_order:
            month_order.append(month)

    month_order = sorted(month_order)
    years = sorted(by_year.keys())
    matrix: list[list[float | None]] = []
    for year in years:
        matrix.append([by_year[year].get(month) for month in month_order])

    return {"months": month_order, "years": years, "values": matrix}


async def build_curated_instruments() -> list[dict[str, str]]:
    futures_payload = await moex_get_json(
        "/engines/futures/markets/forts/securities.json",
        params={"iss.only": "securities", "securities.columns": "SECID,SHORTNAME,ASSETCODE"},
    )
    futures = rows_to_dicts(futures_payload, "securities")
    items: list[dict[str, str]] = []
    seen: set[str] = set()

    for item in futures:
        secid = str(item.get("SECID", "")).upper()
        if not secid or secid in seen:
            continue
        shortname = item.get("SHORTNAME")
        label = build_display_name(secid, str(shortname) if shortname else None, None, secid.endswith("F"))
        items.append({"secid": secid, "label": label})
        seen.add(secid)

    for secid, label in TOP_STOCKS:
        if secid not in seen:
            items.append({"secid": secid, "label": label})
            seen.add(secid)

    items.sort(key=lambda item: item["secid"])
    return items


@app.get("/health")
@app.get("/api/health")
def health() -> dict[str, str]:
    ensure_schema()
    return {"status": "ok"}


@app.get("/instruments")
@app.get("/api/instruments")
async def instruments() -> dict[str, Any]:
    items = await build_curated_instruments()
    return {"items": items}


@app.post("/analyze")
@app.post("/api/analyze")
async def analyze(request: AnalyzeRequest) -> dict[str, Any]:
    ensure_schema()

    ticker1 = normalize_ticker(request.ticker1)
    ticker2 = normalize_ticker(request.ticker2)
    if ticker1 == ticker2:
        raise HTTPException(status_code=400, detail="Choose two different tickers")

    instrument1 = await resolve_instrument(ticker1)
    instrument2 = await resolve_instrument(ticker2)

    available_starts = [value for value in [request.start_date, instrument1.start_date, instrument2.start_date] if value]
    effective_start = max(available_starts)

    today = datetime.now(timezone.utc).date()
    end_candidates = [today]
    for instrument in (instrument1, instrument2):
        if instrument.category == "futures" and not instrument.perpetual and instrument.expiration_date:
            end_candidates.append(instrument.expiration_date)
    effective_end = min(end_candidates)

    if effective_start > effective_end:
        raise HTTPException(status_code=400, detail="No overlapping period is available for the selected pair")

    await sync_candles(instrument1, effective_start, effective_end)
    await sync_candles(instrument2, effective_start, effective_end)

    series1 = fetch_series(instrument1, effective_start, effective_end)
    series2 = fetch_series(instrument2, effective_start, effective_end)
    spread_series = align_spread(series1, series2)

    if not spread_series:
        raise HTTPException(status_code=404, detail="No overlapping candle points were found for the selected pair")

    spread_values = [row["spread"] for row in spread_series]
    monthly_stats_rows = monthly_statistics(spread_series)
    seasonality = build_seasonality(monthly_stats_rows)

    return {
        "instrument1": {
            "secid": instrument1.secid,
            "label": instrument1.display_name,
            "start_date": instrument1.start_date.isoformat() if instrument1.start_date else None,
            "expiration_date": instrument1.expiration_date.isoformat() if instrument1.expiration_date else None,
        },
        "instrument2": {
            "secid": instrument2.secid,
            "label": instrument2.display_name,
            "start_date": instrument2.start_date.isoformat() if instrument2.start_date else None,
            "expiration_date": instrument2.expiration_date.isoformat() if instrument2.expiration_date else None,
        },
        "effective_start": effective_start.isoformat(),
        "effective_end": effective_end.isoformat(),
        "summary": {
            "median_spread": median(spread_values),
            "observations": len(spread_values),
        },
        "series": {
            "instrument1": series1,
            "instrument2": series2,
            "spread": spread_series,
        },
        "monthly_stats": monthly_stats_rows,
        "seasonality": seasonality,
    }
