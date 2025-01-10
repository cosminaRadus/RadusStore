from airflow.models import BaseOperator
from google.cloud import bigquery
from airflow.utils.decorators import apply_defaults
from google.api_core.exceptions import NotFound


class LoadFactTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, project_id, dataset_id, table_id, sql_query, *args, **kwargs):
        super(LoadFactTableOperator, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.sql_query = sql_query

    def execute(self, context):
        client = bigquery.Client(project=self.project_id)
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        try:
            client.get_table(table_ref)
        except NotFound:
            self.log.error(f"Table {table_ref} not found. Please create the table first.")
            return
        
        job_config = bigquery.QueryJobConfig()
        job = client.query(self.sql_query, job_config=job_config)

        job.result()
        self.log.info(f"Data loaded into {table_ref} successfully.")


