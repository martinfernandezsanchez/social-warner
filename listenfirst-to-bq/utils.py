import os
import json
import lfapi
import logging
from google.cloud import secretmanager, bigquery

def initialize_services():
    """
    Initialize logging, Firebase Admin SDK, and Firestore.
    
    Returns:
        dict: A dictionary containing initialized clients (Firestore, BigQuery).
    """
    # 1. Initialize Logging
    logger = initialize_logging()

    # 2. Initialize BigQuery Client
    bq_client = initialize_bigquery()

    return {
        'logger': logger,
        'bigquery': bq_client
    }

def initialize_logging():
    """
    Configure the logging settings for the application.
    """
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    print(json.dumps({
        "severity": "DEBUG",
        "message": "Logging has been initialized."
    }))

    logger = logging.getLogger(__name__)

    return logger

def initialize_bigquery():
    """
    Initialize and return the BigQuery client.
    
    Returns:
        bigquery.Client: Initialized BigQuery client.
    """
    bq_client = bigquery.Client()
    print(json.dumps({
        "severity": "DEBUG",
        "message": "BigQuery client initialized."
    }))
    return bq_client

def get_secret(secret_name):
    """Retrieve a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_data = response.payload.data.decode("UTF-8")
    return secret_data

def authenticate_lf_client():
    """Authenticate and return the LF client."""
    lf_credentials = get_secret('LF_CREDENTIALS')
    lf_credentials_json = json.loads(lf_credentials)
    auth = lfapi.Auth(lf_credentials_json["client_id"], lf_credentials_json["client_secret"])
    client = lfapi.Client(lf_credentials_json["api_key"], auth)
    return client