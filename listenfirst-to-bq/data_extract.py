import os, json
from flask import jsonify

from utils import authenticate_lf_client, initialize_services
from data_extract import extract_data_from_api
from data_transform import transform_data
from data_load import load_data_to_bq

# Get Cloud Function variables
listenfirst_client_context = os.getenv('LISTENFIRST_CLIENT_CONTEXT')
write_disposition = os.getenv('WRITE_DISPOSITION')
bigquery_dataset = os.getenv('BIGQUERY_DATASET')

# Initialize services
services = initialize_services()
logger = services['logger']
bq_client = services['bigquery']

def listenfirst_to_bq(request):
    """HTTP Cloud Function to export reports.
    
    Args:
        json: JSON with optional configs designed for testing

    Returns:
        tuple: Response message and HTTP status code.
    """
    try:
        print(json.dumps({
            "severity": "INFO",
            "message": f"listenfirst-to-bq-dev called with request: {request}"
        }))

        # Get request values
        export_config = request.get_json().get('export_config')
        table_name = request.get_json().get('table_name')
        start_date = request.get_json().get('start_date')
        end_date = request.get_json().get('end_date')

        # Authenticate LF client
        client = authenticate_lf_client()
        print(json.dumps({
            "severity": "INFO",
            "message": "Authenticated with Listen First services."
        }))

        try:
            fetch_data = {
                "dataset_id": export_config["dataset_id"],
                "metrics": list(export_config["metrics"].keys()),
                "group_by": list(export_config["group_by"].keys()),
                "meta_dimensions": list(export_config["meta_dimensions"].keys()),
                "brands": export_config["brands"]
            }

            raw_data = extract_data_from_api(client, listenfirst_client_context, fetch_data, start_date, end_date)

            if raw_data is None:
                print(json.dumps({
                    "severity": "WARNING",
                    "message": f"No data returned for config ID: {table_name}"
                }))

            transformed_data = transform_data(raw_data, export_config)
            print(json.dumps({
                "severity": "DEBUG",
                "message": f"transformed_data: {transformed_data}"
            }))

            # Load data into BigQuery
            load_data_to_bq(
                bq_client=bq_client,
                transformed_data=transformed_data,
                dataset_id=bigquery_dataset,
                table_name=table_name,
                write_disposition=write_disposition
            )
            print(json.dumps({
                "severity": "INFO",
                "message": f"Successfully processed config ID: {table_name}"
            }))

        except Exception as e:
            print(json.dumps({
                "severity": "ERROR",
                "message": f"Error processing config ID {table_name}: {e}"
            }))

        return jsonify({
            "message": f"Processed export configurations successfully."
        }), 200

    except Exception as e:
        print(json.dumps({
            "severity": "CRITICAL",
            "message": f"Function failed: {e}"
        }))
        return jsonify({"error": "Internal Server Error"}), 500