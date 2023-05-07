# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-csv

import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account

keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
PROJECT_ID = "kinetic-genre-384501"
client = bigquery.Client(
    project=PROJECT_ID,
    credentials=credentials,
)

partition_list = {"events":"20210210","orders":"20210210","users":"20201023"}
no_partition_list = ["addresses","order_items","products","promos"]
custer_list = [[],[],["first_name", "last_name"]]

def load_data_from_file(table,clustering_fields):
    """load data from file with partition and custering"""
    if clustering_fields:
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=clustering_fields,
        )
    else:
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
    file_path = f"data/{table}.csv"
    with open(file_path, "rb") as files:
        table_id = f"{PROJECT_ID}.deb_bootcamp.{table}${partition_list[table]}"
        job = client.load_table_from_file(files, table_id, job_config=job_config)
        return job.result()

def load_data_from_file_no_partition(table):
    """load data from file"""
    job_config_non_partition = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    file_path = f"data/{table}.csv"
    with open(file_path, "rb") as files:
        table_id = f"{PROJECT_ID}.deb_bootcamp.{table}"
        job = client.load_table_from_file(files, table_id, job_config=job_config_non_partition)
        return job.result()

res = map(load_data_from_file,list(partition_list.keys()),custer_list)
print(list(res))

res_no_partition = map(load_data_from_file_no_partition,no_partition_list)
print(list(res_no_partition))
