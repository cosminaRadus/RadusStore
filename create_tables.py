from google.cloud import bigquery

client = bigquery.Client()

sql_file = 'create_tables.sql'

with open(sql_file, 'r') as file:
    sql_queries = file.read()

try:
    for query in sql_queries.split(';'):
        query = query.strip()
        if query:
            client.query(query).result() # .result() = wait for the completion of a query job
except Exception as e:
     print(f"Error executing queries: {e}")
    
