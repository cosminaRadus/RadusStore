import requests


def generate_dim_data(file_name, api_key):
    url =  f'https://my.api.mockaroo.com/{file_name}?key={api_key}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if response.status_code == 200:
                if 'text/csv' in response.headers.get('Content-Type'):
                    return response.text
    except requests.exceptions.Timeout:
        raise Exception("API request timed out")
    except requests.exceptions.HTTPError as err:
        raise Exception(f"API request failed: {err}")
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    
def upload_to_gcs(client, bucket_name, data, gcs_file_name):
    
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_file_name)

    if blob.exists():
        existing_data = blob.download_as_text()
        existing_lines = existing_data.splitlines()

        new_header = data.splitlines()[0]

        if existing_lines[0] != new_header:
            final_data = existing_data + data
        else:
            new_data_without_header = '\n'.join(data.splitlines()[1:])
            final_data = existing_data + new_data_without_header
    else:
            final_data = data

    blob.upload_from_string(final_data, content_type='text/csv')

