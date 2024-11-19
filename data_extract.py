import os
import logging
import pandas as pd
from lfapi import Client

def extract_data_from_api(client: Client, config: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extract data from the Listen First API based on the provided configuration,
    handling pagination as necessary.

    Args:
        client (Client): Authenticated Listen First client instance.
        config (dict): Configuration dictionary containing parameters for the API call.
            Expected keys:
                - dataset_id (str): Identifier for the dataset.
                - metrics (list): List of metrics to retrieve.
                - group_by (list): List of dimensions to group the data by.
                - meta_dimensions (list): List of meta dimensions.
                - brands (list): List of brand IDs to filter the data.
                - page_size (int, optional): Number of records per page. Defaults to 1000.

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
        logging.error(error_msg)
        raise ValueError(error_msg)

    # Compute start_date and end_date
    try:
        start_date = start_date or "{{nDaysAgo 7}}"
        end_date = end_date or "{{maxDateAvailable}}"
    except Exception as e:
        logging.error(f"Error computing dates: {e}")
        raise RuntimeError(f"Error computing dates: {e}") from e

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
        }],
        "start_date": start_date,
        "end_date": end_date
    }

    aggregated_data = []  # To store all fetched data

    logging.info(f"Initiating API call with query: {query}")

    try:
        async_page_gen = client.async_analytic_query(query,
                                                     client_context=os.getenv('CLIENT_CONTEXT', 'Social team test query'),
                                                     max_rows=int(os.getenv('MAX_ROWS', 1000)))
        for i, page in enumerate(async_page_gen):
            aggregated_data.append(page.to_pandas())

        data = pd.concat(aggregated_data)

        logging.info(f"Total records fetched: {len(aggregated_data)}.")
        logging.info(f"Aggregated_data: {aggregated_data}.")
        return client.fetch(json=query).to_pandas()

    except Exception as e:
        logging.error(f"API call failed: {e}")
        raise RuntimeError(f"API call failed: {e}") from e