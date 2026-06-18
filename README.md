Project Title: End-to-End eSewa Transaction Analytics ETL Pipeline

The Mission: "Designed and built an automated data pipeline to ingest synthetic eSewa digital wallet streams, clean transaction statuses, calculate high-level business metrics (volume/success rates), and persist logs into a relational database layer for analytical usage."

Tech Stack: Apache Airflow, Docker/Docker-Compose, PostgreSQL, Python (Psycopg2, CSV).

Architecture Flow: 1. Extract: Simulates raw eSewa transaction feeds, capturing amounts, modes, and transaction statuses into a structured file layer.

2. Transform: Leverages decoupled Airflow workers to read raw streams, isolate operational patterns, and parse metrics utilizing secure cross-task state memory (XComs).

3. Load: Automatically ensures database schema execution and commits batch records into a decoupled metadata database warehouse.
