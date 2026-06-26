# End-to-End Fintech Transaction Analytics ETL Pipeline

An enterprise-grade, multi-stage data engineering pipeline modeled after real-world digital wallet streaming metrics. This project containerizes an orchestration layer using **Apache Airflow**, automates transaction processing logic, computes live operational metadata, and structures relational analytics persistence within a **PostgreSQL** database.

---

## 🏛️ Pipeline Architecture & Lineage

The pipeline decouples ingestion, business logic transformation, and analytical persistence into an isolated, fault-tolerant DAG (Directed Acyclic Graph) workflow:



1. **Extract (`extract_raw_data`)**: Simulates a high-frequency transaction stream from a digital wallet core API (capturing Transaction IDs, Amounts in NPR, Payment Modes, and transaction success/failure statuses) and appends logs into a local file storage layer.
2. **Transform (`transform_and_aggregate`)**: Parses raw ingest logs sequentially. Utilizes Airflow Task Instance memory sharing (**XComs**) to securely pass calculated runtime business metrics (Total Successful Volume, Total Success/Failure Counts) across isolated worker contexts.
3. **Load (`load_into_sql`)**: Uses Python's native PostgreSQL client (`psycopg2`) to dynamically execute DDL schema mappings, create operational ledger tables if non-existent, and commit incremental batch metrics directly to the analytical database.

---

## 🛠️ Tech Stack & Infrastructure

* **Orchestration:** Apache Airflow 2.x (DAG Workflow Engine)
* **Database Engine:** PostgreSQL (Relational Data Store)
* **Containerization & Networking:** Docker & Docker-Compose
* **Core Language & Drivers:** Python 3.x, Psycopg2, CSV Parser

---

## 🚀 Key Engineering Selling Points (Interview-Ready)

* **Modular & Decoupled Task Isolation:** Rather than executing logic in a single monolithic script, tasks are strictly isolated. If the database engine encounters network latency or downtime, the raw ingestion layer remains entirely unaffected.
* **State Management via XComs:** Avoided heavy database reads/writes by utilizing secure internal worker memory structures to transport lightweight transactional aggregates between distinct workflow life-cycles.
* **Idempotency Focus:** The loading phase utilizes transactional commits (`conn.commit()`) alongside `CREATE TABLE IF NOT EXISTS` constraints, ensuring the database engine maintains integrity over repetitive workflow executions.
* **Full Containerization:** Bypassed systemic "works on my machine" friction by wrapping the entire environment inside shared network volumes governed via Docker.

---

## 📊 Verification & Production Output

When executed via the Airflow scheduler, the pipeline builds an incremental transaction timeline. Querying the internal core database ledger displays structured analytics summaries detailing operational transaction performance:

```sql
-- Connect to the core database and inspect tracking metrics
SELECT * FROM  wallet_batch_metrics;

Analytical Database Table State:
id |       run_timestamp       | successful_transactions | failed_transactions | total_volume_npr 
----+---------------------------+-------------------------+---------------------+------------------
  1 | 2026-06-18 07:47:55       |                      29 |                   6 |           233424
  2 | 2026-06-18 07:51:12       |                       4 |                   1 |            34150
  3 | 2026-06-18 07:52:40       |                       3 |                   2 |            19800
(3 rows)

⚙️ Local Development Setup
1. Clone the Repository:
git clone [https://github.com/DileshworiJ/digital-wallet-transaction-etl-pipeline.git](https://github.com/DileshworiJ/digital-wallet-transaction-etl-pipeline.git)
cd digital-wallet-transaction-etl-pipeline

2. Initialize Environment & Start Containers:
docker compose up -d

3. Access Control Center:
Open http://localhost:8080 to trigger, monitor, and audit pipeline tasks live via the Apache Airflow web interface.
