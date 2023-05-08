import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "kinetic-genre-384501-9d633532a56c.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "kinetic-genre-384501"

# set client storage gcs
bucket_name = "deb-bootcamp-100037"
storage_client = storage.Client(
    project=project_id,
    credentials=credentials,
)
bucket = storage_client.bucket(bucket_name)

# set client BigQuery
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials,
    location=location,
)

partition_list = {"events":"20210210","orders":"20210210","users":"20201023"}

custering_list = {"users":["first_name", "last_name"]}

def load_data_from_gcs_to_bigquery(filename,destination_blob_name):

    """load data to BigQuery"""

    # decare table id
    table_id = f"{project_id}.deb_raw_zone.{filename.split('.')[0]}"
    print("table_id : ", table_id)

    # check key in config partition list
    if filename.split(".")[0] in partition_list.keys():

        # replace table id with partition id
        table_id = f"{table_id}${partition_list[filename.split('.')[0]]}"
        print("table_id : ", table_id)

        # set BigQuery config with partition
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
        )

        # check key in config custering list
        if filename.split(".")[0] in custering_list.keys():

            # set BigQuery config with partition and custering
            job_config = bigquery.LoadJobConfig(
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="created_at",
                ),
                clustering_fields=custering_list[filename.split(".")[0]],
            )

    else :

        # set BigQuery config no partition
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
        )

        # check key in config custering list
        if filename.split(".")[0] in custering_list.keys():

            # set BigQuery config custering
            job_config = bigquery.LoadJobConfig(
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                clustering_fields=custering_list[filename.split(".")[0]],
            )

    print("loading data to BigQuery")
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()
    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def upload_file_csv_to_gcs(filename):

    """upload data"""

    try :
        print("filename : ", filename)
        file_path = f"{DATA_FOLDER}/{filename}"
        print("file_path : ", file_path)
        if filename.split(".")[0] in partition_list.keys():
            destination_blob_name = f"{BUSINESS_DOMAIN}/{filename.split('.')[0]}/{partition_list[filename.split('.')[0]]}/{filename}"
        else:
            destination_blob_name = f"{BUSINESS_DOMAIN}/{filename.split('.')[0]}/{filename}"
        print("destination_blob_name : ", destination_blob_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        load_data_from_gcs_to_bigquery(filename,destination_blob_name)
        return {filename:"Successed"}
    except Exception as exc:
        return {filename:exc}

data_path = os.listdir(DATA_FOLDER)

res = map(upload_file_csv_to_gcs,data_path)

print(list(res))