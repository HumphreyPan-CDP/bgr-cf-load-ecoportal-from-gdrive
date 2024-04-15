import io
from datetime import datetime
import re
import google.auth
import json
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import Conflict
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import functions_framework

DOWDLOADED_FLAG = "downloaded"

def search_file(query, creds):
    """Search file in drive location
    """
    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)
        files = []
        page_token = None
        while True:
            # pylint: disable=maybe-no-member
            # response = service.files().list(q=query, spaces='drive', fields='nextPageToken, ' 'files(id, name, modifiedTime, description)', pageToken=page_token).execute()
            response = service.files().list(q=query, spaces='drive', 
                                            fields='nextPageToken, ' 'files(id, name, modifiedTime, description, webContentLink, webViewLink, capabilities)', 
                                            pageToken=page_token).execute()
            for file in response.get('files', []):
                print(f'Found file: {file.get("name")}, {file.get("id")}, {file.get("description")}, {file.get("capabilities")}')
                # print(f'Found file: {file.get("name")}, {file.get("id")}, {file.get("description")}')
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    except HttpError as error:
        print(f'\t An error occurred: {error}')
        files = None
    return files

def download_file_from_drive(file_id, creds):
    try:
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f'\t Download {int(status.progress() * 100)}.')
    except HttpError as error:
        print(f'\t An error occurred: {error}')
        file = None
    return file.getvalue()

def upload_file_to_bkt(bkt_project, bkt_name, file_name, file):
    """Downloads a file save to bucket
    """
    storage_client = storage.Client(bkt_project)
    bucket = storage_client.bucket(bkt_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(file)
    print(f"\t file {file_name} uploaded to {bkt_name}")

def create_bigquery_extranl_table(project, dataset, table_name, file_prefix, external_source_format):
    client = bigquery.Client(project)
    dataset_id = f"{project}.{dataset}"

    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = "australia-southeast1"
    try: 
        dataset_ref = client.create_dataset(dataset_ref, timeout=30)  # Make an API request.
        print(f"Created dataset: {dataset_id}")
    except Conflict:
        print(f"alread existed dataset: : {dataset_id}")

    table_id = f"{dataset_id}.{table_name}"
    # client.delete_table(table_id, not_found_ok=True)

    external_source_format = external_source_format
    source_uris = [f"{file_prefix}/*"]
    source_uri_prefix = file_prefix

    external_config = bigquery.ExternalConfig(external_source_format)
    external_config.source_uris = source_uris
    external_config.autodetect = True

    hive_partitioning_opts = bigquery.HivePartitioningOptions()
    hive_partitioning_opts.mode = "AUTO"
    hive_partitioning_opts.require_partition_filter = False
    hive_partitioning_opts.source_uri_prefix = source_uri_prefix
    external_config.hive_partitioning = hive_partitioning_opts

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    try:
        table = client.create_table(table)
        print(f"table created: {table}")
    except Conflict:
        print(f"table existed: {table}")

def normalise_column_name(col_names):
    result = [re.sub('[^0-9a-zA-Z]+', '_', col.lower()) for col in col_names]
    result = [re.sub('^[0-9]+', '', col) for col in result]
    result = [re.sub('[^0-9a-zA-Z]+$', '', col) for col in result]
    result= [re.sub('^[^0-9a-zA-Z]+', '', col) for col in result]
    return result

def rename_gdrive_file(file_id, new_name, creds):
    try: 
        service = build('drive', 'v3', credentials=creds)
        body = {'name': new_name}
        service.files().update(fileId=file_id, body=body).execute()
        print(f"\t file {file_id}| is renamed to {new_name}")
    except HttpError as error:
        print(f'\t An error occurred when renaming file: {file_id}')

@functions_framework.http
def load_flie_from_gdrive(request):    
    try: 
        request_json = request.get_json(silent=True)
        gdrive_folder_id = request_json['gdrive_folder_id']
        landing_project = request_json['landing_project']
        target_bucket_name = request_json['target_bucket_name']
        target_archive_bucket_name = request_json['target_archive_bucket_name']
        target_tables = request_json['target_tables'].split(',') 
        target_dataset_name = request_json['target_dataset_name']
        target_default_partition = request_json['target_default_partition']

        # gdrive_folder_id = "1V9txunznrUuhMiNNfE57V8_5lUj-g316"
        # landing_project = "bgr-d-landing-b23c"
        # target_bucket_name = "bgr-d-landing-file-history"
        # target_archive_bucket_name = "bgr-d-ecoportal-archive"
        # target_tables = ["incidents", "meetings"]
        # target_dataset_name = "ecoportal"
        # target_default_partition = "1900-01-01"

        print(f"source google drive folder id: {gdrive_folder_id}")
        print(f"target landing_project: {landing_project}")
        print(f"target bucket_name: {target_bucket_name}")
        print(f"target_archive_bucket_name: {target_archive_bucket_name}")
        print(f"target_tables: {target_tables}")
        print(f"target_dataset_name: {target_dataset_name}")
        print(f"target_default_partition: {target_default_partition}")

    except Exception as e:
        error_msg = f"input paras errors: {str(e)}"
        print(error_msg)
        response = {
            "message": error_msg
        }
        return (response, 400)
    
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds, project_id = google.auth.default(scopes=scopes)

        # from google.oauth2 import service_account
        # creds = service_account.Credentials.from_service_account_file('key.json')
        
        # use folder id to find files
        query= f"'{gdrive_folder_id}' in parents"
        files = search_file(query, creds)

        log = {}

        for table in target_tables:
            print(f"dealing with file for table: {table}".center(150, '*'))
            files_to_load = [file for file in files if (
                (table in file['name'].lower()) and (file['name'].startswith(DOWDLOADED_FLAG)==False)
                )
                ]
            
            if len(files_to_load)==0:
                print(f"no new files for table: {table}")
                log[table] = "no new files"
            else:
                log[table] = ",".join([file['name'] for file in files_to_load])
                for file in files_to_load:
                    file_name = file['name']
                    print(f"dealing file: {file_name}".center(100, '*'))
                    
                    # parse hive partition value from file suffix
                    name_suffix = file_name.lower().replace('.csv', '').split('-')[-1]
                    try:
                        partition = datetime.strptime(name_suffix, '%Y%m%d').date().strftime('%Y-%m-%d')
                    except ValueError:
                        print(f"\t name_suffix of file {file_name} is not a date, will put file in to default partition {target_default_partition}")
                        partition = target_default_partition

                    # download file from Google drive
                    file_data = download_file_from_drive(file['id'], creds)

                    # save raw csv to archive bucket
                    upload_file_to_bkt(landing_project, target_archive_bucket_name, f"{table}/{file_name}", file_data)

                    # remove special character from column names
                    file_df = pd.read_csv(io.StringIO(file_data.decode("utf-8")))
                    file_df.columns = normalise_column_name(file_df.columns)
                    file_df = file_df.astype(str)
                    print(f"\t rows_in_file: {len(file_df)}")

                    # save file to hive-partition bucket
                    target_file_name = f"{table}/ingestion_date={partition}/{file_name.lower().replace('.csv', '.parquet')}"
                    target_blob = f"gs://{target_bucket_name}/{target_file_name}"
                    file_df.to_parquet(target_blob)
                    print(f"\t file {target_file_name} uploaded to {target_blob}")

                    rename_gdrive_file(file['id'], f'{DOWDLOADED_FLAG}_{file_name}', creds)

                # create bigquery external table
                file_prefix = f"gs://{target_bucket_name}/{table}"
                create_bigquery_extranl_table(landing_project, target_dataset_name, table, file_prefix, "PARQUET")
        return (log, 200)
    
    except Exception as e:
        print( {str(e)})
        return (str(e), 500)