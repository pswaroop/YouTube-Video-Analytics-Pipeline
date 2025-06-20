from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args = {
    'owner':'swaroop',
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='youtube_trending_data_pipeline',
    default_args=default_args,
    start_date=datetime(2025,6,15),
    schedule='@once', #'@daily'
    catchup=False,
    tags=['youtube','etl'],
    doc_md="""
    ### YouTube Trending ETL
    1. Scrape trending data from YouTube API
    2. Process it using PySpark
    """
) as dag:
    run_scraper = BashOperator(
        task_id='scrape_youtube_data',
        bash_command='cd /absolute/path/to/project && python3 scraper/youtube_scraper.py'
    )

    run_processor = BashOperator(
        task_id='process_raw_data',
        bash_command='cd /absolute/path/to/project && python3 spark_jobs/process_data.py'
    )

    run_scraper >> run_processor