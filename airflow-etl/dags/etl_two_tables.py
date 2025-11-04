from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import os
import shutil
import traceback

# -----------------------------
# Constants / Config
# -----------------------------
POSTGRES_URI = os.environ.get(
    "POSTGRES_CONN_URI",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)
DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/output"

default_args = {"owner": "joseph", "retries": 1, "retry_delay": timedelta(minutes=2)}

# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="etl_movies_ratings",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Ingest two related datasets, transform in parallel, merge → Postgres, analyze, cleanup",
    tags=["demo", "etl", "parallel"],
) as dag:

    engine = create_engine(POSTGRES_URI, pool_pre_ping=True)

    # -------- Ingestion (parallel) --------
    def ingest_movies():
        df = pd.read_csv(f"{DATA_DIR}/movies.csv")
        df["title"] = df["title"].str.strip()
        df["year"] = df["year"].astype(int)
        df["movie_id"] = df["movie_id"].astype(int)
        with engine.begin() as conn:
            df.to_sql("stg_movies", conn, if_exists="replace", index=False)
        print("Movies ingested:", len(df))

    def ingest_ratings():
        df = pd.read_csv(f"{DATA_DIR}/ratings.csv")
        df["rating"] = df["rating"].astype(float)
        df["movie_id"] = df["movie_id"].astype(int)
        with engine.begin() as conn:
            df.to_sql("stg_ratings", conn, if_exists="replace", index=False)
        print("Ratings ingested:", len(df))

    with TaskGroup(group_id="ingest_transform_parallel") as tg:
        t_movies = PythonOperator(task_id="ingest_movies", python_callable=ingest_movies)
        t_ratings = PythonOperator(task_id="ingest_ratings", python_callable=ingest_ratings)

    # -------- Merge & load final --------
    def merge_transform():
        with engine.begin() as conn:
            m = pd.read_sql("select * from stg_movies", conn)
            r = pd.read_sql("select * from stg_ratings", conn)
        r["rating2"] = r["rating"] ** 2
        merged = r.merge(m, on="movie_id", how="inner")
        with engine.begin() as conn:
            merged.to_sql("fact_movie_ratings", conn, if_exists="replace", index=False)
        print("Merged records:", len(merged))

    merge = PythonOperator(task_id="merge_and_load_final", python_callable=merge_transform)

    # -------- Validate via Python (hits Postgres) --------
    def validate_rowcount_py():
        with engine.begin() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM fact_movie_ratings").scalar()
        if not cnt:
            raise ValueError("fact_movie_ratings is empty!")
        print(f"Rowcount OK: {cnt}")

    validate = PythonOperator(task_id="validate_rowcount", python_callable=validate_rowcount_py)

   # -------- Analysis (Top 10 plot) --------
    def simple_analysis():
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        try:
            # 1) Query Postgres
            with engine.begin() as conn:
                q = """
                select 
                    title,
                    round( avg(rating)::numeric, 2 ) as avg_rating,
                    count(*) as n
                from fact_movie_ratings
                group by title
                order by avg_rating desc, n desc
                """
                df = pd.read_sql(q, conn)

            print("Analysis row count:", len(df))
            if df.empty:
                raise ValueError("Analysis query returned 0 rows — check upstream merge.")

            # 2) Save full CSV
            out_csv = f"{OUTPUT_DIR}/avg_ratings.csv"
            df.to_csv(out_csv, index=False)
            print("Wrote full CSV:", out_csv)

            # 3) Plot Top 10 (safe in headless env)
            try:
                import matplotlib
                matplotlib.use("Agg")
                import matplotlib.pyplot as plt

                # Select top 10 movies by avg_rating
                top10 = df.head(10).sort_values("avg_rating", ascending=True)

                plt.figure(figsize=(10, 6))
                plt.barh(top10["title"].astype(str), top10["avg_rating"].astype(float), color="skyblue")
                plt.xlabel("Average Rating")
                plt.title("Top 10 Movies by Average Rating")
                plt.tight_layout()

                out_png = f"{OUTPUT_DIR}/avg_ratings_top10.png"
                plt.savefig(out_png)
                print("Wrote PNG:", out_png)
            except Exception as e:
                print("Plotting skipped due to error:", repr(e))

        except Exception as e:
            print("perform_analysis failed:", repr(e))
            traceback.print_exc()
            raise


    analyze = PythonOperator(task_id="perform_analysis", python_callable=simple_analysis)

    # -------- Cleanup staging tables --------
    def cleanup_intermediate():
        with engine.begin() as conn:
            conn.exec_driver_sql("drop table if exists stg_movies")
            conn.exec_driver_sql("drop table if exists stg_ratings")
        print("Dropped staging tables stg_movies and stg_ratings.")

    cleanup_db = PythonOperator(task_id="cleanup_stage_db", python_callable=cleanup_intermediate)

    # -------- Cleanup files --------
    def cleanup_files():
        tmp_dir = "/opt/airflow/tmp"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            os.makedirs(tmp_dir, exist_ok=True)
            print(f"Reset tmp directory: {tmp_dir}")

        # Optionally remove temp data
        for f in os.listdir(DATA_DIR):
            if f.endswith(".tmp"):
                os.remove(os.path.join(DATA_DIR, f))
                print(f"Deleted tmp file: {f}")

    cleanup_files_task = PythonOperator(task_id="cleanup_files", python_callable=cleanup_files)

    # -------- DAG dependencies --------
    tg >> merge >> validate >> analyze >> [cleanup_db, cleanup_files_task]
