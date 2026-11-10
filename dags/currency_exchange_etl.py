"""
Currency Exchange Rate ETL pipeline
===================================

Extract  -> daily exchange rates from the Frankfurter API
Transform -> reshape each rate into the required MongoDB document
Load     -> upsert the documents into MongoDB (currency_db.exchange_rates)

Data moves between tasks with NAMED XComs (explicit keys):
    extract_exchange_rates   pushes key "raw_exchange_rates"
    transform_exchange_rates pulls "raw_exchange_rates", pushes "transformed_exchange_rates"
    load_exchange_rates      pulls "transformed_exchange_rates", pushes "load_summary"
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from pymongo import MongoClient, ReplaceOne

from include.currency_utils import fetch_rates, order_document, transform_rates

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (can be overridden with environment variables in .env)
# ---------------------------------------------------------------------------
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "EUR")
QUOTE_CURRENCIES = os.getenv("QUOTE_CURRENCIES", "USD,GBP,JPY,PKR,CNY,AUD,CAD,CHF").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys
XCOM_RAW = "raw_exchange_rates"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY = "load_summary"

default_args = {
    "owner": "mlops",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rates: Frankfurter API -> transform -> MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb", "mlops"],
)
def currency_exchange_etl():

    @task(task_id="extract_exchange_rates")
    def extract(**context):
        """Task 1: call the Frankfurter API and push the raw JSON as a named XCom."""
        raw = fetch_rates(BASE_CURRENCY, QUOTE_CURRENCIES)
        log.info("Extracted raw payload from Frankfurter API: %s", raw)

        context["ti"].xcom_push(key=XCOM_RAW, value=raw)

    @task(task_id="transform_exchange_rates")
    def transform(**context):
        """Task 2: pull raw data, reshape each rate into the required document format."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW)
        if not raw:
            raise ValueError(f"No data found in XCom key '{XCOM_RAW}'")

        documents = transform_rates(raw, base=BASE_CURRENCY)
        if not documents:
            raise ValueError("Transformation produced no documents")

        for doc in documents:
            log.info("Transformed: %s", doc)

        ti.xcom_push(key=XCOM_TRANSFORMED, value=documents)

    @task(task_id="load_exchange_rates")
    def load(**context):
        """Task 3: pull transformed docs and upsert them into MongoDB."""
        ti = context["ti"]
        documents = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED)
        if not documents:
            raise ValueError(f"No data found in XCom key '{XCOM_TRANSFORMED}'")

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]

            # one document per (base, target, date) -> re-runs don't create duplicates
            collection.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
                name="uniq_pair_per_day",
            )

            # XCom serialization does not keep dict key order, so re-apply the
            # required field order, then ReplaceOne (not $set) stores each
            # document exactly in that shape.
            documents = [order_document(d) for d in documents]
            operations = [
                ReplaceOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                    },
                    d,
                    upsert=True,
                )
                for d in documents
            ]
            result = collection.bulk_write(operations)

            summary = {
                "database": MONGO_DB,
                "collection": MONGO_COLLECTION,
                "documents_received": len(documents),
                "inserted": result.upserted_count,
                "updated": result.modified_count,
                "matched": result.matched_count,
                "total_in_collection": collection.count_documents({}),
            }
            log.info("Load summary: %s", summary)
            ti.xcom_push(key=XCOM_LOAD_SUMMARY, value=summary)
        finally:
            client.close()

    extract() >> transform() >> load()


currency_exchange_etl()
