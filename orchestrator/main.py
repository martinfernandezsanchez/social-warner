import os
import json
import requests
from typing import List
from datetime import datetime
from google.oauth2 import id_token
from google.auth.transport.requests import Request

from utils import initialize_storage, parse_date_input, load_file_from_bucket, call_bq_rutine

# Get Cloud Function variables
environment = os.getenv('ENVIRONMENT')
storage_file = os.getenv('STORAGE_FILE')
lf_to_bq_url = os.getenv('LF_TO_BQ_URL')

# Initialize the storage client
storage_client = initialize_storage()

def execute_incremental_update(request):
    """
    Cloud Function that invokes another Cloud Function by authenticating with the default Service Account.
    
    Expects a JSON payload with the following structure:
    {
        "start_date": "YYYY-MM-DD" or "{{nDaysAgo N}}",
        "end_date": "YYYY-MM-DD" or "{{nDaysAgo N}}",
        "table_filter": ["table1", "table2"]  // Optional
    }
    """
    try:
        # Parse the JSON body from the request
        request_json = request.get_json()
        if not request_json:
            return {
                "status": "error",
                "message": "Invalid request: JSON body is missing."
            }, 400
        
        # Extract and validate start_date and end_date
        start_date_input = request_json.get('start_date')
        end_date_input = request_json.get('end_date')
        table_filter = request_json.get('table_filter', None)  # Optional
        
        if not start_date_input or not end_date_input:
            return {
                "status": "error",
                "message": "Missing required parameters: start_date and end_date."
            }, 400
        
        # Parse the date inputs
        start_date = parse_date_input(start_date_input)
        end_date = parse_date_input(end_date_input)
        
        if not start_date:
            return {
                "status": "error",
                "message": f"Invalid start_date format: {start_date_input}. Expected 'YYYY-MM-DD' or '{{nDaysAgo N}}'."
            }, 400
        
        if not end_date:
            return {
                "status": "error",
                "message": f"Invalid end_date format: {end_date_input}. Expected 'YYYY-MM-DD' or '{{nDaysAgo N}}'."
            }, 400
        
        if start_date > end_date:
            return {
                "status": "error",
                "message": "start_date cannot be after end_date."
            }, 400
        
        # Retrieve export configurations from storage
        file_content = load_file_from_bucket(
            storage_client,
            bucket_name="warner-listenfirst-to-bq",
            source_blob_name=storage_file
        )
        
        # Process the file content (e.g., parse JSON)
        export_config_docs = json.loads(file_content)
        print(json.dumps({
            "severity": "INFO",
            "message": "Fetched export configurations from Storage."
        }))
        print(json.dumps({
            "severity": "DEBUG",
            "message": f"export_config_docs: {export_config_docs}"
        }))
        
        # If table_filter is provided, filter the export_config_docs
        if table_filter:
            if not isinstance(table_filter, list):
                return {
                    "status": "error",
                    "message": "table_filter must be an array of table names."
                }, 400
            filtered_export_config_docs = {
                key: value for key, value in export_config_docs.items() if key in table_filter
            }
            print(json.dumps({
                "severity": "INFO",
                "message": f"Filtered export_config_docs: {filtered_export_config_docs}"
            }))
        else:
            # If no filter is provided, include all tables
            filtered_export_config_docs = export_config_docs
        
        processed_count = 0
        total_tables = len(filtered_export_config_docs)
        
        for table_name, export_config_doc in filtered_export_config_docs.items():
            print(json.dumps({
                "severity": "INFO",
                "message": f"Processing config ID: {table_name}"
            }))
        
            try:
                # Generate an access token using the default service account
                token = id_token.fetch_id_token(Request(), lf_to_bq_url)
        
                # Headers with the authentication token
                headers = {
                    "Authorization": f"Bearer {token}"
                }
                
                # Prepare the export configuration payload
                export_config = {
                    "dataset_id": export_config_doc["dataset_id"],
                    "metrics": export_config_doc["metrics"],
                    "group_by": export_config_doc["group_by"],
                    "meta_dimensions": export_config_doc["meta_dimensions"],
                    "brands": export_config_doc["brands"]
                }
        
                # Prepare the payload for the POST request
                payload = {
                    "export_config": export_config,
                    "table_name": table_name,
                    "start_date": start_date,
                    "end_date": end_date
                }
                
                response = requests.post(lf_to_bq_url, json=payload, headers=headers)
        
                if response.status_code == 200:
                    processed_count += 1
                    print(json.dumps({
                        "severity": "INFO",
                        "message": f"Successfully processed table: {table_name}"
                    }))
                else:
                    print(json.dumps({
                        "severity": "ERROR",
                        "message": f"Failed to process table: {table_name}, Status: {response.status_code}, Response: {response.text}"
                    }))
            
            except Exception as e:
                print(json.dumps({
                    "severity": "ERROR",
                    "message": f"Error processing config ID {table_name}: {e}"
                }))
        
        print(f"Processed {processed_count} out of {total_tables} export configurations successfully.")
        
        if environment == 'prod':
            call_bq_rutine()
        
        return {
            "status": "success",
            "message": f"Cloud Function executed successfully. Processed {processed_count} out of {total_tables} tables."
        }, 200

    except Exception as general_exception:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Unexpected error: {general_exception}"
        }))
        return {
            "status": "error",
            "message": f"An unexpected error occurred: {general_exception}"
        }, 500