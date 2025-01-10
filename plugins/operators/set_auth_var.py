import json
from airflow.models import Variable

# Load your service account JSON file
with open('/Users/kokorad/Desktop/RadusStore/plugins/operators/potent-app-439210-c8-8f380062bc88.json') as f:
    service_account_dict = json.load(f)

# Set the variable in Airflow
service_account_json = json.dumps(service_account_dict)
Variable.set("gcs_credentials", service_account_json)

print("Variable 'gcs_credentials' has been set successfully.")

