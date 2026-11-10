# Currency Exchange Rate ETL — Airflow (Astro CLI) + MongoDB

Daily ETL pipeline that pulls currency exchange rates from the **Frankfurter API**,
transforms them, and loads them into **MongoDB**, with **Mongo Express** as the web admin UI.

```
Frankfurter API ──► extract_exchange_rates ──► transform_exchange_rates ──► load_exchange_rates ──► MongoDB
                     XCom: raw_exchange_rates   XCom: transformed_exchange_rates   XCom: load_summary
```

## Project structure

```
currency-etl/
├── dags/currency_exchange_etl.py     # DAG with 3 tasks + named XComs
├── include/currency_utils.py         # API call + transform logic
├── tests/                            # unit tests + DAG integrity test
├── docker-compose.override.yml       # MongoDB + Mongo Express (network + volumes)
├── Dockerfile                        # Astro Runtime image
├── requirements.txt                  # pymongo, requests
└── .env                              # Mongo URI, currencies
```

## Requirements covered

| Requirement | Where |
|---|---|
| ETL pipeline with Astro CLI | `Dockerfile`, `dags/` |
| Frankfurter API (`/v2/rates?base=EUR&quotes=...`) | `include/currency_utils.py → fetch_rates` |
| MongoDB primary database | `mongodb` service in `docker-compose.override.yml` |
| Mongo Express as another Docker service | `mongo-express` service |
| Docker networking | both services join the `airflow` bridge network, so Airflow reaches Mongo at host `mongodb` |
| Persistent storage | named volumes `currency_mongo_data` (`/data/db`) and `currency_mongo_config` |
| 3 tasks: Extract / Transform / Load | `extract_exchange_rates`, `transform_exchange_rates`, `load_exchange_rates` |
| Required document shape | `transform_rates()` → `base_currency, target_currency, exchange_rate, rate_date, source, retrieved_at` |
| Named XCom | `ti.xcom_push(key=...)` / `ti.xcom_pull(task_ids=..., key=...)` |

## How to run

Prerequisites: Docker Desktop running, Astro CLI installed (`winget install -e --id Astronomer.Astro` on Windows, `brew install astro` on macOS).

```bash
cd currency-etl
astro dev start
```

| UI | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin (or no login, depending on CLI version) |
| Mongo Express | http://localhost:8081 | admin / admin123 |

Then in Airflow: unpause **currency_exchange_etl** → click **Trigger**.

> If `astro dev start` complains about the image tag, run `astro dev init` in an empty folder and copy the `FROM` line from its Dockerfile into this one.

Run the tests:
```bash
astro dev pytest
```

Check the data from the terminal:
```bash
docker exec -it currency_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin \
  --eval 'db.getSiblingDB("currency_db").exchange_rates.find().pretty()'
```

Verify networking / volumes:
```bash
docker network ls
docker volume ls | grep currency
docker inspect currency_mongodb --format '{{json .NetworkSettings.Networks}}'
```

## Screenshots for the report

1. Project structure in VS Code
2. Code: `currency_exchange_etl.py`, `currency_utils.py`, `docker-compose.override.yml`
3. Terminal output of `astro dev start` (all containers running) and `docker ps`
4. `docker network ls` and `docker volume ls` output
5. Airflow UI: DAG graph view, all 3 tasks green
6. Logs of each task (extract shows raw JSON, transform shows documents, load shows summary)
7. Airflow **XCom** tab of each task showing the keys `raw_exchange_rates`, `transformed_exchange_rates`, `load_summary`
8. Mongo Express: `currency_db` → `exchange_rates` collection with documents
9. `astro dev pytest` passing

## Push to GitHub

```bash
git init
git add .
git commit -m "Airflow ETL: Frankfurter API to MongoDB with Mongo Express"
git branch -M main
git remote add origin https://github.com/<your-username>/currency-exchange-etl.git
git push -u origin main
```
