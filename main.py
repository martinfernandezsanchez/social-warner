import os, json
import logging
from flask import jsonify

from utils import authenticate_lf_client, initialize_services
from data_extract import extract_data_from_api
from data_transform import transform_data
from data_load import load_data_to_bq
from data_import_from_storage import load_file_from_bucket

bigquery_dataset = os.getenv('BIGQUERY_DATASET')

# Initialize services
services = initialize_services()
bq_client = services['bigquery']
storage_client = services['storage']


def listenfirst_to_bq(request):
    """HTTP Cloud Function to export reports.
    
    Args:
        json: JSON with optional configs designed for testing

    Returns:
        tuple: Response message and HTTP status code.
    """
    try:
        # Get request values
        write_disposition = request.get_json().get('write_disposition') or os.getenv('WRITE_DISPOSITION')
        listenfirst_client_context = request.get_json().get('listenfirst_client_context') or os.getenv('LISTENFIRST_CLIENT_CONTEXT')
        reports_filter = request.get_json().get('reports_filter')
        start_date = request.get_json().get('start_date')
        end_date = request.get_json().get('end_date')

        # Authenticate LF client
        client = authenticate_lf_client()
        logging.info("Authenticated with Listen First services.")

        # Retrieve export configurations from storage
        file_content = load_file_from_bucket(
            storage_client,
            bucket_name="warner-listenfirst-to-bq",
            source_blob_name="lfm_configurations.json"
        )
        # Process the file content (e.g., parse JSON)
        export_config_docs = json.loads(file_content)
        print('export_config_docs:', export_config_docs)
        logging.info("Fetched export configurations from Storage.")

        processed_count = 0
        for key, export_config_doc in export_config_docs.items():
            config_id = key

            if reports_filter is not None:
                if config_id != reports_filter: continue
            logging.info(f"Processing export config ID: {config_id}")

            try:
                fetch_data = {
                    "dataset_id": export_config_doc["dataset_id"],
                    "metrics": list(export_config_doc["metrics"].keys()),
                    "group_by": list(export_config_doc["group_by"].keys()),
                    "meta_dimensions": list(export_config_doc["meta_dimensions"].keys()),
                    "brands": export_config_doc["brands"]
                }
                raw_data = extract_data_from_api(client, listenfirst_client_context, fetch_data, start_date, end_date)
                if raw_data is None:
                    logging.warning(f"No data returned for config ID: {config_id}")
                    continue

                transform_config = {
                    
                }
                transformed_data = transform_data(raw_data, export_config_doc)
                print('transformed_data', transformed_data)

                # Load data into BigQuery
                load_data_to_bq(
                    bq_client=bq_client,
                    transformed_data=transformed_data,
                    dataset_id=bigquery_dataset,
                    table_name=config_id,
                    write_disposition=write_disposition
                )
                logging.info(f"Successfully processed config ID: {config_id}")
                processed_count += 1

            except Exception as e:
                logging.error(f"Error processing config ID {config_id}: {e}")
                # Optionally, handle specific exceptions or implement retry logic

        return jsonify({
            "message": f"Processed {processed_count} export configurations successfully."
        }), 200

    except Exception as e:
        logging.critical(f"Function failed: {e}")
        return jsonify({"error": "Internal Server Error"}), 500