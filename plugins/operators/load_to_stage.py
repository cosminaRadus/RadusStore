import sys

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import storage
from google.oauth2 import service_account
from generate_data.generate_data import generate_dim_data, upload_to_gcs

sys.path.append('/Users/kokorad/Desktop/RadusStore/')


class MockarooToGSCOperator(BaseOperator):
    @apply_defaults
    def __init__(self, api_key, file_name, bucket_name, credentials_file, *args, **kwargs):
        super(MockarooToGSCOperator, self).__init__(*args, **kwargs)
        self.api_key = api_key
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.credentials_file = credentials_file

    def execute(self, context):

        data = generate_dim_data(self.file_name, self.api_key)
       
        credentials = service_account.Credentials.from_service_account_file(self.credentials_file)
        client = storage.Client(credentials=credentials)

        if data:
            upload_to_gcs(client, self.bucket_name, data, self.file_name)

