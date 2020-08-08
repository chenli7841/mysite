from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'bigquery.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

datasets = list(client.list_datasets())



for ds in datasets:
    print(ds.dataset_id)
