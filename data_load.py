import os
import logging
from typing import List, Dict
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.api_core.exceptions import GoogleAPIError

def load_data_to_bq(
    bq_client: bigquery.Client,
    transformed_data: List[Dict],
    dataset_id: str,
    table_name: str,
    write_disposition: str
):
    """
    Load transformed data into a BigQuery table within a specific dataset.

    Args:
        bq_client (bigquery.Client): Authenticated BigQuery client.
        transformed_data (List[Dict]): List of dictionaries representing transformed data.
        dataset_id (str): BigQuery dataset ID.
        table_name (str): BigQuery table name.

    Raises:
        ValueError: If transformed_data is empty.
        RuntimeError: If the load job fails.
    """
    if not transformed_data:
        error_msg = "No data provided to load into BigQuery."
        logging.error(error_msg)
        raise ValueError(error_msg)

    # Construct the fully qualified table ID
    table_id = f"{bq_client.project}.{dataset_id}.000_warner_dl_lf_{table_name}"

    try:
        # Define load job configuration
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            #schema_update_options="ALLOW_FIELD_ADDITION",
            autodetect=True
        )

        logging.info(f"Starting load job for table {table_id} with {len(transformed_data)} records.")

        # Initiate load job
        load_job = bq_client.load_table_from_json(
            transformed_data,
            table_id,
            job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()
        logging.info(f"Successfully loaded {load_job.output_rows} rows into {table_id}.")

    except GoogleAPIError as e:
        logging.error(f"Google API error occurred while loading data into {table_id}: {e}")
        raise RuntimeError(f"Google API error: {e}") from e
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading data into {table_id}: {e}")
        raise RuntimeError(f"Unexpected error: {e}") from e