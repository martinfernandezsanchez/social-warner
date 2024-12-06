import os
import json
import lfapi
import logging
import firebase_admin
from firebase_admin import firestore
from google.cloud import secretmanager, bigquery, storage

def initialize_services():
    """
    Initialize logging, Firebase Admin SDK, and Firestore.
    
    Returns:
        dict: A dictionary containing initialized clients (Firestore, BigQuery).
    """
    # 1. Initialize Logging
    initialize_logging()

    # 2. Initialize Firebase Admin SDK
    db = initialize_firebase()

    # 3. Initialize BigQuery Client
    bq_client = initialize_bigquery()

    # 4. Initialize Storage Client
    storage_client = initialize_storage()

    return {
        'firestore': db,
        'bigquery': bq_client,
        'storage': storage_client
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
    logging.info("Logging has been initialized.")

def initialize_firebase():
    """
    Initialize Firebase Admin SDK and return Firestore client.
    
    Returns:
        firestore.Client: Initialized Firestore client.
    """
    if not firebase_admin._apps:
        # Initialize Firebase Admin SDK
        firebase_admin.initialize_app()
        logging.info("Firebase Admin SDK initialized.")
    else:
        logging.info("Firebase Admin SDK already initialized.")

    # Initialize Firestore client
    db = firestore.Client(database=os.getenv('FIRESTORE_DATABASE'))
    return db

def initialize_bigquery():
    """
    Initialize and return the BigQuery client.
    
    Returns:
        bigquery.Client: Initialized BigQuery client.
    """
    bq_client = bigquery.Client()
    logging.info("BigQuery client initialized.")
    return bq_client

def initialize_storage():
    """
    Initialize and return the Google Cloud Storage client.
    
    Returns:
        storage.Client: Initialized Storage client.
    """
    storage_client = storage.Client()
    logging.info("Google Cloud Storage client initialized.")
    return storage_client

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