"""
Helper functions for the currency exchange-rate ETL pipeline.

Kept separate from the DAG file so the logic can be unit-tested
without spinning up Airflow.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

API_BASE_URL = "https://api.frankfurter.dev"
SOURCE_NAME = "Frankfurter API"

# Required document fields, in the exact order they must appear in MongoDB
DOCUMENT_FIELDS = (
    "base_currency",
    "target_currency",
    "exchange_rate",
    "rate_date",
    "source",
    "retrieved_at",
)


def order_document(doc: dict) -> dict:
    """Return a copy of doc with exactly the required fields, in the required order.

    Airflow's XCom serialization does not preserve dict key order, so the load
    task re-applies it before writing to MongoDB.
    """
    return {field: doc[field] for field in DOCUMENT_FIELDS}


def fetch_rates(base: str, quotes: list[str], timeout: int = 30) -> Any:
    """Call Frankfurter v2 /rates and return the parsed JSON body.

    Example call:
        https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
    """
    url = f"{API_BASE_URL}/v2/rates"
    params = {"base": base, "quotes": ",".join(quotes)}
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _normalise(raw: Any, base: str) -> list[dict]:
    """Turn the API payload into a list of {base, quote, rate, date} rows.

    Frankfurter v2 returns a flat array, one object per quote:
        [{"date": "2026-11-06", "base": "EUR", "quote": "USD", "rate": 1.15}, ...]
    The older/dict style is also supported for robustness:
        {"base": "EUR", "date": "2026-11-06", "rates": {"USD": 1.15, ...}}
    """
    if isinstance(raw, list):
        return [
            {
                "base": row.get("base", base),
                "quote": row["quote"],
                "rate": row["rate"],
                "date": row["date"],
            }
            for row in raw
        ]

    if isinstance(raw, dict) and "rates" in raw:
        return [
            {
                "base": raw.get("base", base),
                "quote": quote,
                "rate": rate,
                "date": raw["date"],
            }
            for quote, rate in raw["rates"].items()
        ]

    raise ValueError(f"Unexpected API response format: {raw!r}")


def transform_rates(raw: Any, base: str, retrieved_at: str | None = None) -> list[dict]:
    """Convert the raw API payload into the required document shape:

    {
      "base_currency": "EUR",
      "target_currency": "USD",
      "exchange_rate": 1.15,
      "rate_date": "2026-11-04",
      "source": "Frankfurter API",
      "retrieved_at": "2026-11-05T07:15:00"
    }
    """
    if retrieved_at is None:
        retrieved_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat()

    documents = []
    for row in _normalise(raw, base):
        rate = row["rate"]
        if rate is None or float(rate) <= 0:
            # skip invalid / missing rates instead of loading bad data
            continue
        documents.append(
            {
                "base_currency": str(row["base"]).upper(),
                "target_currency": str(row["quote"]).upper(),
                "exchange_rate": round(float(rate), 6),
                "rate_date": str(row["date"])[:10],
                "source": SOURCE_NAME,
                "retrieved_at": retrieved_at,
            }
        )
    return documents
