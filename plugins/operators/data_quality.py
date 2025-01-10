import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.oauth2 import service_account
from google.cloud import bigquery
from helpers.sql_queries import SqlQueries

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self, credentials_dict, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.credentials_dict = credentials_dict


    def execute(self, context):
        print('hei sunt aici')
        credentials = service_account.Credentials.from_service_account_info(self.credentials_dict)
        client = bigquery.Client(credentials=credentials)

        sql_queries = SqlQueries(column_id="id", project_id="potent-app-439210-c8", dataset_id="radusStore", table="fact_product_popularity")

    
        # ROW NUMBER CHECK -- SKIPPED
        # if self.check_row_count(client, sql_queries):
        #     self.log.info('The correct number of rows have been inserted')
        # else:
        #     raise ValueError("Data quality check failed for row count.")
        
        # DUPLICATE ROWS CHECK
        if self.check_uniqueness(client, sql_queries):
            self.log.info('No duplicated rows found')
        else:
            raise ValueError("Duplicated rows found.")
        
        # NULL VALUES CHECK
        if self.check_null_values(client).any():
            raise ValueError(f"The following columns contain NULL values: {self.check_null_values[self.check_null_values > 0]}")
        else:
            self.log.info("No NULL values found in the table.")

        # RATING VALUES CHECK
        if self.check_rating(client, sql_queries):
            self.log.info("Ratings in parameters.")
        else:
            raise ValueError("Ratings exceed parameters.")

        # VALID IDS FOR CUSTOMERS AND PRODUCTS CHECK
        if self.check_existing_ids(client, sql_queries):
            raise ValueError("Invalid customer id(s) and/or product id(s).")
        else:
            self.log.info("No invalid customer ids or product ids.")

        # VALID TIMESTAMP CHECK
        if self.check_valid_timestamp(client, sql_queries):
            self.log.info("No invalid timestamp found.")
        else: 
           raise ValueError("Invalid timestamps found.")

    def check_row_count(self, client, sql_queries):
        fact_row_count = sql_queries.row_count()

        cb_query_result = client.query(sql_queries.row_count_cb_without_view).result() 
        expected_count = int(list(cb_query_result)[0]['row_count']) 
        
        return fact_row_count == expected_count

    def check_uniqueness(self, client, sql_queries):
        no_duplicated_rows = client.query(sql_queries.uniqueness_check_fact).result()

        return no_duplicated_rows.total_rows == 0
    
    def check_null_values(self, client):
        df = client.list_rows('potent-app-439210-c8.radusStore.fact_product_popularity').to_dataframe()
        null_counts = df.isnull().sum()

        return null_counts
    
    def check_rating(self, client, sql_queries):
        max_rating = float(list(client.query(sql_queries.rating_validation()).result())[0]['max_rating'])
        min_rating = float(list(client.query(sql_queries.rating_validation()).result())[0]['min_rating'])

        return min_rating >= 0.0 and max_rating <= 5.0

    def check_existing_ids(self, client, sql_queries):

        null_products = client.query(sql_queries.product_id_check()).result()
        null_customers = client.query(sql_queries.customer_id_check()).result()
        
    
        null_product_rows = null_products.to_dataframe()
        null_customer_rows = null_customers.to_dataframe()
     
        invalid_ids_found = False

        if not null_product_rows.empty:
            self.log.error("Invalid product_ids found:")
            self.log.error(null_product_rows)  
            invalid_ids_found = True

        if not null_customer_rows.empty:
            self.log.error("Invalid customer_ids found:")
            self.log.error(null_customer_rows)  
            invalid_ids_found = True
        
        return invalid_ids_found  

    def check_existing_ids(self, client, sql_queries):
        null_products = client.query(sql_queries.product_id_check).result()
        null_customers = client.query(sql_queries.customer_id_check).result()

        return null_products.total_rows > 0 or null_customers.total_rows > 0
    
    def check_valid_timestamp(self, client, sql_queries):
       
        invalid_timestamps = client.query(sql_queries.timestamp_validity_check()).result()
        
        invalid_timestamp_count = int(list(invalid_timestamps)[0]['invalid_dates'])


        return invalid_timestamp_count == 0




