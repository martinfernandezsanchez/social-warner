import re
import json
import pandas as pd
from datetime import date, timedelta
from lfapi import Client

def extract_data_from_api(client: Client, client_context: str, config: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract data from the Listen First API based on the provided configuration,
    handling pagination as necessary.

    Args:
        client (Client): Authenticated Listen First client instance.
        client_context (str): Message to be registered in LF when the export is made.
        config (dict): Configuration dictionary containing parameters for the API call.
            Expected keys:
                - dataset_id (str): Identifier for the dataset.
                - metrics (list): List of metrics to retrieve.
                - group_by (list): List of dimensions to group the data by.
                - meta_dimensions (list): List of meta dimensions.
                - brands (list): List of brand IDs to filter the data.
                - page_size (int, optional): Number of records per page. Defaults to 1000.
        start_date (str): The starting datetime of the analysis period, expressed in this format YYYY-MM-DD.
        start_date (str): The ending datetime of the analysis period, expressed in this format YYYY-MM-DD.

    Returns:
        dict: Aggregated data retrieved from the API across all pages.

    Raises:
        ValueError: If required configuration fields are missing or invalid.
        RuntimeError: If the API call fails or date computation encounters an error.
    """
    required_fields = ["dataset_id", "metrics", "group_by", "meta_dimensions", "brands"]
    missing_fields = [field for field in required_fields if field not in config]

    if missing_fields:
        error_msg = f"Missing required configuration fields: {', '.join(missing_fields)}"
        print(json.dumps({
            "severity": "ERROR",
            "message": error_msg
        }))
        raise ValueError(error_msg)

    # Initialize query parameters
    query = {
        "dataset_id": config["dataset_id"],
        "metrics": config["metrics"],
        "group_by": config["group_by"],
        "meta_dimensions": config["meta_dimensions"],
        "filters": [{
            "field": "lfm.brand_view.id",
            "operator": "IN",
            "values": config["brands"]
        }]
    }


    # Compute start_date and end_date
    try:
        if "content" in config["dataset_id"]:
            query["filters"].append({
                "field": "lfm.fact.date_str",
                "operator": "BETWEEN",
                "values": [
                    format_date(start_date),
                    format_date(end_date)
                ]
            })

            start_date = "{{nDaysAgo 365}}"
        
        query["start_date"] = start_date
        query["end_date"] = end_date
    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Error computing dates: {e}"
        }))
        raise RuntimeError(f"Error computing dates: {e}") from e

    aggregated_data = []  # To store all fetched data

    print(json.dumps({
        "severity": "INFO",
        "message": f"Initiating API call with query: {query}"
    }))

    try:
        async_page_gen = client.async_analytic_query(query,
                                                     client_context=client_context)
        for i, page in enumerate(async_page_gen):
            aggregated_data.append(page.to_pandas())

        data = pd.concat(aggregated_data)

        print(json.dumps({
            "severity": "DEBUG",
            "message": f"Total records fetched: {len(aggregated_data)}."
        }))
        return data

    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"API call failed: {e}"
        }))
        raise RuntimeError(f"API call failed: {e}") from e

def format_date(lf_date: str) -> str:
    try:
        today = date.today()
        q_days_ago = int(re.findall(r"\{\{nDaysAgo\s+(\d+)\}\}", lf_date)[0])
        parsed_date = (today - timedelta(days=q_days_ago)).strftime('%Y-%m-%d')

        print(json.dumps({
            "severity": "DEBUG",
            "message": f"Parsed date: {parsed_date}"
        }))

        return parsed_date
    
    except Exception as e:
        print(json.dumps({
            "severity": "ERROR",
            "message": f"Date format failed: {e}"
        }))
    """
    Formats the date from the LF format {{nDaysAgo N}} to YYYY-MM-DD.

    Args:
        lf_date (str): date value expressed in the LF format.
        
    Returns:
        str: date expressed in YYYY-MM-DD format.
    """ 