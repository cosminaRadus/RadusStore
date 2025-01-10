import sys
import os
import json
from datetime import timedelta


# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from airflow import DAG
from airflow.utils.dates import days_ago
from plugins.operators.load_to_stage import MockarooToGSCOperator
from airflow.operators.empty import EmptyOperator
from plugins.operators.stage_to_BigQuery import GcsToBigQueryOperator
from plugins.operators.load_fact import LoadFactTableOperator
from plugins.operators.data_quality import DataQualityOperator


default_args = {
    'owner': 'kokorad',
    'start_date': days_ago(1),
    'execution_timeout': timedelta(minutes=2), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_dag2',
    default_args=default_args,
    schedule_interval='@daily',  
) as dag:

    MOCKAROO_API_KEY = "a30f52a0"
    GCS_BUCKET_NAME = 'radusstore'
    CREDENTIALS_FILE = 'plugins/operators/potent-app-439210-c8-8f380062bc88.json'

    with open(CREDENTIALS_FILE) as f:
        credentials_dict = json.load(f)


    file_names = ['customers', 'products', 'orders', 'product_reviews', 'customer_behaviour']
 
    start_task = EmptyOperator(task_id='start')
    end_upload_task = EmptyOperator(task_id='end_upload_task')
    end_load_task = EmptyOperator(task_id='end_load_task')
    end_task = EmptyOperator(task_id='end_task')


    for file_name in file_names:
        upload_task = MockarooToGSCOperator(
            task_id=f'upload_{file_name}',
            api_key=MOCKAROO_API_KEY,
            file_name=file_name,
            bucket_name=GCS_BUCKET_NAME,
            credentials_file=CREDENTIALS_FILE,
        )

        start_task >> upload_task >> end_upload_task


    for file_name in file_names:
        load_gsc_to_bq = GcsToBigQueryOperator(
            credentials_dict=credentials_dict,
            task_id=f'load_gsc_to_bq_{file_name}',
            bucket_name=GCS_BUCKET_NAME,
            gsc_file_name=file_name,
            dataset_id='radusStore',
            table_id=file_name,
        )

        end_upload_task >> load_gsc_to_bq >> end_load_task


    load_fact_table = LoadFactTableOperator(
        task_id='load_fact_data',
        project_id='potent-app-439210-c8',
        dataset_id='radusStore',
        table_id='fact_product_popularity',
        sql_query="""
        INSERT INTO `potent-app-439210-c8.radusStore.fact_product_popularity`
        (product_id, customer_id, product_name, product_category, interaction_type, was_ordered, rating, customer_country, timestamp)
        SELECT 
            pr.product_id,
            cb.customer_id,
            p.product_name,
            p.category,
            cb.event_type,
            CASE 
                WHEN o.product_id IS NOT NULL THEN 'yes'
                ELSE 'no'
            END AS was_ordered,
            ROUND(AVG(pr.rating), 2) AS rating,
            c.country,
            cb.event_timestamp
        FROM 
            `potent-app-439210-c8.radusStore.customer_behaviour` cb
        JOIN 
            `potent-app-439210-c8.radusStore.product_reviews` pr ON cb.product_id = pr.product_id
        JOIN 
            `potent-app-439210-c8.radusStore.products` p ON cb.product_id = p.product_id
        LEFT JOIN 
            `potent-app-439210-c8.radusStore.orders` o ON cb.customer_id = o.customer_id AND cb.product_id = o.product_id
        LEFT JOIN 
            `potent-app-439210-c8.radusStore.customers` c ON cb.customer_id = c.customer_id
        WHERE 
            cb.event_type <> 'view' AND cb.event_timestamp IS NOT NULL AND NOT EXISTS (
                SELECT *
                FROM `potent-app-439210-c8.radusStore.fact_product_popularity` f
                WHERE f.product_id = pr.product_id
                AND f.customer_id = cb.customer_id
                AND f.timestamp = cb.event_timestamp
            )
        GROUP BY 
            pr.product_id,
            cb.customer_id,
            p.product_name,
            p.category,
            cb.event_type,
            o.product_id,
            c.country,
            cb.event_timestamp;
            
    """
    )


    data_quality_task = DataQualityOperator(
        task_id='data_quality_test',
        credentials_dict=credentials_dict,
    )


    end_load_task >> load_fact_table >> data_quality_task >> end_task