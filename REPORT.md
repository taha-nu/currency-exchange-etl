# Currency Exchange Rate ETL Pipeline — Airflow (Astro CLI) + MongoDB

**Course:** MLOps  **Author:** TAHA NUCES (`taha-nu`)
**Repository:** https://github.com/taha-nu/currency-exchange-etl

---

## 1. Introduction

This project is a daily ETL (Extract, Transform, Load) pipeline built with Apache Airflow 3 using
the **Astro CLI** (Astro Runtime `3.1-25`). Every day it:

1. **Extracts** the latest exchange rates for EUR against eight currencies
   (USD, GBP, JPY, PKR, CNY, AUD, CAD, CHF) from the public **Frankfurter API**
   (`https://api.frankfurter.dev/v2/rates?base=EUR&quotes=...`).
2. **Transforms** each rate into a fixed document format.
3. **Loads** the documents into **MongoDB** (`currency_db.exchange_rates`), the primary database.

**Mongo Express** runs as a separate Docker service and gives a web UI for browsing the data.
All services share one Docker network, and MongoDB's data lives in named volumes so it survives
container restarts. The three tasks pass data to each other using **named XComs** (explicit keys).

Each document stored in MongoDB has exactly this shape:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1185,
  "rate_date": "2026-11-10",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-10T01:34:58"
}
```

### Project structure

```
currency-etl/
├── dags/currency_exchange_etl.py     # DAG: 3 tasks + named XComs
├── include/currency_utils.py         # API call, transform logic, document field order
├── tests/
│   ├── dags/test_dag_integrity.py    # DAG loads, has exactly the 3 tasks in order
│   └── test_currency_utils.py        # transform / document-shape unit tests
├── docker-compose.override.yml       # MongoDB + Mongo Express (network + named volumes)
├── Dockerfile                        # Astro Runtime image
├── requirements.txt                  # pymongo, requests
├── .env                              # Mongo URI, base/quote currencies
└── report_assets/                    # saved terminal outputs used in this report
```

[SCREENSHOT 1: Project structure in VS Code (Explorer panel expanded)]

---

## 2. Architecture

```mermaid
flowchart LR
    API["Frankfurter API<br/>api.frankfurter.dev/v2/rates"]

    subgraph NET["Docker bridge network: currency-etl_0f310f_airflow"]
        direction LR
        subgraph AF["Airflow (Astro Runtime 3.1-25)"]
            direction TB
            SCH["scheduler"]
            DP["dag-processor"]
            APIS["api-server :8080"]
            TRG["triggerer"]
            PG[("postgres<br/>Airflow metadata + XCom")]
        end

        subgraph DAG["DAG: currency_exchange_etl (@daily)"]
            direction LR
            E["extract_exchange_rates"]
            T["transform_exchange_rates"]
            L["load_exchange_rates"]
            E -- "XCom key:<br/>raw_exchange_rates" --> T
            T -- "XCom key:<br/>transformed_exchange_rates" --> L
        end

        MDB[("MongoDB 7.0<br/>host: mongodb:27017<br/>currency_db.exchange_rates")]
        ME["Mongo Express 1.0.2<br/>:8081"]
    end

    VOL1[["volume: currency_mongo_data<br/>/data/db"]]
    VOL2[["volume: currency_mongo_config<br/>/data/configdb"]]

    API -- "HTTPS GET" --> E
    L -- "ReplaceOne upsert" --> MDB
    L -. "XCom key: load_summary" .-> PG
    SCH -. runs .-> DAG
    ME -- "mongodb://mongodb:27017" --> MDB
    MDB --- VOL1
    MDB --- VOL2
```

---

## 3. Docker setup

`astro dev start` creates the Airflow containers (postgres, scheduler, dag-processor, api-server,
triggerer) and merges in `docker-compose.override.yml`, which adds MongoDB and Mongo Express.

### 3.1 `docker-compose.override.yml`

```yaml
services:
  mongodb:
    image: mongo:7.0
    container_name: currency_mongodb
    restart: unless-stopped
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin123
      MONGO_INITDB_DATABASE: currency_db
    ports:
      - "27017:27017"
    # ---- Persistent storage ----
    volumes:
      - mongo_data:/data/db
      - mongo_config:/data/configdb
    # ---- Docker networking ----
    networks:
      - airflow
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]
      interval: 10s
      timeout: 5s
      retries: 5

  mongo-express:
    image: mongo-express:1.0.2
    container_name: currency_mongo_express
    restart: unless-stopped
    depends_on:
      mongodb:
        condition: service_healthy
    environment:
      ME_CONFIG_MONGODB_URL: mongodb://admin:admin123@mongodb:27017/?authSource=admin
      ME_CONFIG_BASICAUTH_USERNAME: admin
      ME_CONFIG_BASICAUTH_PASSWORD: admin123
      ME_CONFIG_BASICAUTH: "true"
    ports:
      - "8081:8081"
    networks:
      - airflow

volumes:
  mongo_data:
    name: currency_mongo_data
  mongo_config:
    name: currency_mongo_config

networks:
  airflow:
    driver: bridge
```

[SCREENSHOT 2c: Code — docker-compose.override.yml open in VS Code]

### 3.2 Networking

Both extra services join the `airflow` bridge network that the Astro CLI already creates for its
own containers (Docker names it `currency-etl_0f310f_airflow`). Because they share that network,
the Airflow tasks reach MongoDB by its **service name** `mongodb`
(`mongodb://admin:admin123@mongodb:27017`), and Mongo Express reaches it the same way.
No container IPs are hard-coded.

Verified with `docker network inspect` (full output: `report_assets/04_docker_network_volumes.txt`):

```
$ docker network inspect currency-etl_0f310f_airflow --format '{{range .Containers}}{{.Name}}  {{.IPv4Address}}{{println}}{{end}}'
currency_mongodb  172.19.0.3/16
currency-etl_0f310f-dag-processor-1  172.19.0.8/16
currency-etl_0f310f-scheduler-1  172.19.0.7/16
currency-etl_0f310f-triggerer-1  172.19.0.5/16
currency-etl_0f310f-api-server-1  172.19.0.6/16
currency_mongo_express  172.19.0.4/16
currency-etl_0f310f-postgres-1  172.19.0.2/16

$ docker inspect currency_mongodb --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}  aliases={{$v.Aliases}}{{end}}'
currency-etl_0f310f_airflow  aliases=[currency_mongodb mongodb]

$ docker exec currency-etl_0f310f-scheduler-1 python -c "...MongoClient('mongodb://admin:admin123@mongodb:27017/?authSource=admin')...ping"
ping from scheduler -> {'ok': 1.0}
```

### 3.3 Persistent storage (named volumes)

| Volume | Mounted at | Purpose |
|---|---|---|
| `currency_mongo_data` | `/data/db` | MongoDB data files |
| `currency_mongo_config` | `/data/configdb` | MongoDB config metadata |

Because these are named volumes rather than anonymous ones, `astro dev stop` / `astro dev restart`
keep the data. Only `docker volume rm` deletes it.

```
$ docker volume ls --filter name=currency
DRIVER    VOLUME NAME
local     currency-etl_0f310f_airflow_logs
local     currency-etl_0f310f_postgres_data
local     currency_mongo_config
local     currency_mongo_data
```

### 3.4 Running containers

```
$ docker ps --filter name=currency
NAMES                                 IMAGE                                STATUS                    PORTS
currency-etl_0f310f-dag-processor-1   currency-etl_0f310f/airflow:latest   Up                        
currency-etl_0f310f-scheduler-1       currency-etl_0f310f/airflow:latest   Up                        
currency-etl_0f310f-api-server-1      currency-etl_0f310f/airflow:latest   Up                        127.0.0.1:8080->8080/tcp
currency-etl_0f310f-triggerer-1       currency-etl_0f310f/airflow:latest   Up                        
currency_mongo_express                mongo-express:1.0.2                  Up                        0.0.0.0:8081->8081/tcp
currency-etl_0f310f-postgres-1        postgres:15                          Up                        127.0.0.1:5432->5432/tcp
currency_mongodb                      mongo:7.0                            Up (healthy)              0.0.0.0:27017->27017/tcp
```

[SCREENSHOT 3: Terminal — `astro dev start` output (Airflow UI URL shown) and `docker ps` with all 7 containers Up]

[SCREENSHOT 4: Terminal — `docker network ls` and `docker volume ls` output]

---

## 4. The DAG and its three tasks

The DAG `currency_exchange_etl` runs `@daily` (start date 2026-11-01, `catchup=False`,
2 retries with a 2-minute delay). The tasks run in a strict chain:

```python
extract() >> transform() >> load()
```

Configuration and the XCom key names are defined once at the top of the DAG file:

```python
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "EUR")
QUOTE_CURRENCIES = os.getenv("QUOTE_CURRENCIES", "USD,GBP,JPY,PKR,CNY,AUD,CAD,CHF").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys
XCOM_RAW = "raw_exchange_rates"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY = "load_summary"
```

[SCREENSHOT 2a: Code — dags/currency_exchange_etl.py open in VS Code]

[SCREENSHOT 5: Airflow UI — DAG graph view, all 3 tasks green]

### 4.1 Task 1 — Extract Currency Exchange Rates (`extract_exchange_rates`)

Calls the Frankfurter v2 `/rates` endpoint and pushes the raw JSON under the key
`raw_exchange_rates`.

```python
@task(task_id="extract_exchange_rates")
def extract(**context):
    """Task 1: call the Frankfurter API and push the raw JSON as a named XCom."""
    raw = fetch_rates(BASE_CURRENCY, QUOTE_CURRENCIES)
    log.info("Extracted raw payload from Frankfurter API: %s", raw)

    context["ti"].xcom_push(key=XCOM_RAW, value=raw)
```

`include/currency_utils.py`:

```python
API_BASE_URL = "https://api.frankfurter.dev"

def fetch_rates(base: str, quotes: list[str], timeout: int = 30) -> Any:
    url = f"{API_BASE_URL}/v2/rates"
    params = {"base": base, "quotes": ",".join(quotes)}
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()
```

The API returns a flat array, one object per quote currency
(`report_assets/05_frankfurter_api_response.txt`):

```json
[
  {"date": "2026-11-10", "base": "EUR", "quote": "AUD", "rate": 1.6231},
  {"date": "2026-11-10", "base": "EUR", "quote": "GBP", "rate": 0.83424},
  {"date": "2026-11-09", "base": "EUR", "quote": "PKR", "rate": 309.74},
  {"date": "2026-11-10", "base": "EUR", "quote": "USD", "rate": 1.1184}
]
```

Note that the date is per row (PKR's latest published rate was a day older than the others),
so the transform reads the date from each row rather than assuming one date for the whole response.

[SCREENSHOT 6a: Airflow UI — extract_exchange_rates task log showing the raw JSON payload]

### 4.2 Task 2 — Transform Exchange Rate Data (`transform_exchange_rates`)

Pulls `raw_exchange_rates`, converts every row to the required document shape, and pushes the
list under `transformed_exchange_rates`. Rows with a missing or non-positive rate are skipped.

```python
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
```

`include/currency_utils.py`:

```python
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

def transform_rates(raw: Any, base: str, retrieved_at: str | None = None) -> list[dict]:
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
```

`_normalise()` accepts the v2 array format and, for robustness, the older
`{"base": ..., "date": ..., "rates": {...}}` format.

[SCREENSHOT 2b: Code — include/currency_utils.py open in VS Code]

[SCREENSHOT 6b: Airflow UI — transform_exchange_rates task log showing the 8 transformed documents]

### 4.3 Task 3 — Load Data into MongoDB (`load_exchange_rates`)

Pulls `transformed_exchange_rates`, upserts the documents into `currency_db.exchange_rates`, and
pushes a summary under `load_summary`.

```python
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
```

Design choices:

- **Idempotent loads.** A unique index on `(base_currency, target_currency, rate_date)` plus
  upserts means re-running the DAG for the same day updates the existing documents instead of
  creating duplicates.
- **Exact document shape.** During testing I found that Airflow 3's XCom serialization does not
  preserve dictionary key order. A `$set` upsert also writes the filter fields first. Together these
  stored the fields in a scrambled order. The load task now rebuilds every document in the required
  order with `order_document()` and writes it with `ReplaceOne`, so MongoDB stores exactly
  `base_currency, target_currency, exchange_rate, rate_date, source, retrieved_at`.

[SCREENSHOT 6c: Airflow UI — load_exchange_rates task log showing the load summary]

---

## 5. Named XCom usage

Every task pushes and pulls with an **explicit key** through `ti.xcom_push(key=...)` and
`ti.xcom_pull(task_ids=..., key=...)`. No task relies on the implicit `return_value` XCom
(all three tasks return `None`).

| Producer task | XCom key | Value | Consumer task |
|---|---|---|---|
| `extract_exchange_rates` | `raw_exchange_rates` | raw API response (list of 8 rows) | `transform_exchange_rates` |
| `transform_exchange_rates` | `transformed_exchange_rates` | list of 8 documents in the required shape | `load_exchange_rates` |
| `load_exchange_rates` | `load_summary` | dict with inserted / updated / total counts | (final output, visible in the UI) |

XCom entries recorded for the verified run (`report_assets/07_xcom_keys.txt`):

```
XCom entries for dag_id=currency_exchange_etl run_id=manual__2026-11-10T01:34:56.183703+00:00

task_id                    key                          value
----------------------------------------------------------------------------------------------------
extract_exchange_rates     raw_exchange_rates           list[8]
transform_exchange_rates   transformed_exchange_rates   list[8]
load_exchange_rates        load_summary                 dict

Required named XCom keys present: True ['load_summary', 'raw_exchange_rates', 'transformed_exchange_rates']
```

[SCREENSHOT 7a: Airflow UI — XCom tab of extract_exchange_rates showing key raw_exchange_rates]

[SCREENSHOT 7b: Airflow UI — XCom tab of transform_exchange_rates showing key transformed_exchange_rates]

[SCREENSHOT 7c: Airflow UI — XCom tab of load_exchange_rates showing key load_summary]

---

## 6. Results

### 6.1 DAG runs

The DAG was unpaused and triggered from the CLI:

```
astro dev run dags unpause currency_exchange_etl
astro dev run dags trigger currency_exchange_etl
```

All runs finished with state `success`, and every task succeeded on its first attempt
(`report_assets/06_dag_run_status.txt`):

```
$ astro dev run tasks states-for-dag-run currency_exchange_etl manual__2026-11-10T01:34:56.183703+00:00
task_id                  | state   | start_date                       | end_date
=========================+=========+==================================+=================================
extract_exchange_rates   | success | 2026-11-10T01:34:57.334620+00:00 | 2026-11-10T01:34:57.687707+00:00
transform_exchange_rates | success | 2026-11-10T01:34:58.395004+00:00 | 2026-11-10T01:34:58.598511+00:00
load_exchange_rates      | success | 2026-11-10T01:34:59.467663+00:00 | 2026-11-10T01:34:59.681711+00:00
```

Load summary from that run:

```
{'database': 'currency_db', 'collection': 'exchange_rates', 'documents_received': 8,
 'inserted': 0, 'updated': 8, 'matched': 8, 'total_in_collection': 8}
```

`inserted: 0, updated: 8` is expected: earlier runs the same day had already inserted the 8
documents, and this run updated them in place. The collection still holds exactly 8 documents,
which confirms the loads are idempotent.

### 6.2 Data in MongoDB

Queried with `mongosh` inside the `currency_mongodb` container
(`report_assets/07_mongodb_documents.txt`):

```
$ docker exec currency_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet \
    --eval 'db.getSiblingDB("currency_db").exchange_rates.find().sort({target_currency: 1})'
count: 8
{
  _id: ObjectId('6af273df32fb76d1df70473d'),
  base_currency: 'EUR',
  target_currency: 'USD',
  exchange_rate: 1.1185,
  rate_date: '2026-11-10',
  source: 'Frankfurter API',
  retrieved_at: '2026-11-10T01:34:58'
}
```

| base_currency | target_currency | exchange_rate | rate_date | source | retrieved_at |
|---|---|---|---|---|---|
| EUR | AUD | 1.6231 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | CAD | 1.5752 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | CHF | 0.9161 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | CNY | 7.4569 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | GBP | 0.83426 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | JPY | 176.97 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | PKR | 309.76 | 2026-11-09 | Frankfurter API | 2026-11-10T01:34:58 |
| EUR | USD | 1.1185 | 2026-11-10 | Frankfurter API | 2026-11-10T01:34:58 |

(`retrieved_at` is in UTC.)

[SCREENSHOT 8: Mongo Express — currency_db → exchange_rates collection listing the 8 documents]

### 6.3 Tests

`astro dev pytest` runs the unit tests and the DAG integrity test inside the Astro Runtime image
(`report_assets/07_astro_dev_pytest.txt`):

```
tests/dags/test_dag_integrity.py::test_dag_loads_with_three_tasks PASSED [ 20%]
tests/test_currency_utils.py::test_v2_array_format PASSED                [ 40%]
tests/test_currency_utils.py::test_dict_format_supported PASSED          [ 60%]
tests/test_currency_utils.py::test_field_order_matches_required_shape PASSED [ 80%]
tests/test_currency_utils.py::test_invalid_rates_skipped PASSED          [100%]

============================== 5 passed in 1.77s ===============================
✔ All tests passed!
```

[SCREENSHOT 9: Terminal — `astro dev pytest` with 5 passed]

### 6.4 Issues found and fixed during setup

| Issue | Fix |
|---|---|
| Project had no `.astro/` folder (the Astro CLI's project config) | Generated `.astro/config.yaml` with `astro dev init` in a temporary folder and copied it in (gitignored, so each clone regenerates it) |
| Documents were stored with the fields in a scrambled order (XCom doesn't keep key order; `$set` upserts write filter fields first) | Added `DOCUMENT_FIELDS` + `order_document()` and switched the load to `ReplaceOne(upsert=True)`; added a unit test |
| DAG integrity test failed under Airflow 3 (`DagBag.get_dag()` queries the empty test database) | Read the parsed DAG from `dagbag.dags` instead |

---

## 7. GitHub repository

**https://github.com/taha-nu/currency-exchange-etl**

To run it:

```bash
git clone https://github.com/taha-nu/currency-exchange-etl.git
cd currency-exchange-etl
astro dev start
astro dev run dags unpause currency_exchange_etl
astro dev run dags trigger currency_exchange_etl
```

| UI | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | none needed locally (Astro dev mode) |
| Mongo Express | http://localhost:8081 | admin / admin123 |
