import datetime
import logging
import json

import pytz

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from job_seeker.downloader import JobSeeker

parameters = { 
               "where" : "All Adelaide SA",
               "keywords" : "data analyst",
               }


def main(mytimer: func.TimerRequest) -> None:
    tz = pytz.timezone("Australia/Adelaide")
    local_timestamp = datetime.datetime.now(tz=tz).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f'Job function trigger function ran at {local_timestamp}')

    default_credential = DefaultAzureCredential()

    secret_client = SecretClient(
        vault_url="https://junqueira-42.vault.azure.net/",
        credential= default_credential
    )

    storage_credentials = secret_client.get_secret(name="storage-key")

    js = JobSeeker(params=parameters)

    df = js.jobs_df

    output = df.to_csv(encoding = "utf-8", index=False)

    service = BlobServiceClient(account_url="https://storage4223.blob.core.windows.net", credential=storage_credentials.value)

    container_client = service.get_container_client("jobsdata")

    filename = f"data_analyst_jobs_on_{local_timestamp}.csv"

    container_client.upload_blob(name=filename, data=output, blob_type="BlockBlob")

    container_client = service.get_container_client("jobsdatadetails")

    file_name_jobs_detail = f"data_analyst_jobs_detail_on_{local_timestamp}.json"

    job_details_obj = js.get_jobs_detail_json()

    data = json.dumps(job_details_obj)

    container_client.upload_blob(name=file_name_jobs_detail, data=data, blob_type="BlockBlob")


