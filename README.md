# YouTube-Video-Analytics-Pipeline
YouTube Trending Data Pipeline with Scraper, PySpark & Airflow

# Project Overview
Build a daily pipeline that ingests trending YouTube videos (from public datasets or APIs), performs transformations using PySpark, and orchestrates the workflow using Airflow DAGs. The pipeline computes top categories, trending tags, growth in views/likes, and writes clean data to a data warehouse or Data Lake (e.g., Delta Lake, Postgres, or Parquet on S3/Local).

# Pipeline stages:

- Daily Scheduled DAG

- Task 1: Scrape trending data from YouTube (API)

- Task 2: Clean and normalize with PySpark

- Task 3: Compute analytics (top categories, growth, etc.)

- Task 4: Store in analytics-ready format (Delta/Parquet/Postgres)

- (Optional) Notify or trigger a dashboard update

# Commands to Initialize DAG in Airflow (>=2.90)

For creating metadatabase
> airflow db migrate

For running airflow
> airflow standalone

