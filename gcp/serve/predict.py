from io import StringIO
from flask import Flask, jsonify, request
from google.cloud import storage
import joblib
import os
import logging
import gcsfs
import pandas as pd
from keras.models import load_model

# Configure logging
gs_log_file_path = 'gs://us-east1-composer-airflow-7e8e089d-bucket/logs/flask_log_file.log'

def configure_logging():
    # Configure logging to console and a custom StringIO handler
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    log_stream = StringIO()
    file_handler = logging.StreamHandler(log_stream)
    logging.getLogger().addHandler(file_handler)

configure_logging()

fs = gcsfs.GCSFileSystem(project='wegf-mlops')
MODEL_DIR = os.environ.get("AIP_STORAGE_URI")

def upload_log_to_gcs(log_messages, gcs_log_path):
    """
    Upload log messages to Google Cloud Storage.

    Args:
        log_messages (str): The log messages to be uploaded.
        gcs_log_path (str): The GCS path to upload the log messages.

    Returns:
        None
    """
    try:
        with fs.open(gcs_log_path, 'wb') as f:
            f.write(log_messages.encode())
        logging.info(f"Log messages successfully uploaded to {gcs_log_path}")
    except Exception as e:
        logging.error(f"Error uploading log messages to GCS: {e}")
        raise

app = Flask(__name__)

def initialize_variables():
    """
    Initialize environment variables.
    Returns:
        tuple: The project id and bucket name.
    """
    # Initialize environment variables.
    logging.info("Initializing environment variables.")
    project_id = os.getenv("PROJECT_ID")
    bucket_name = os.getenv("BUCKET_NAME")
    return project_id, bucket_name

def initialize_client_and_bucket(bucket_name):
    """
    Initialize a storage client and get a bucket object.
    Args:
        bucket_name (str): The name of the bucket.
    Returns:
        tuple: The storage client and bucket object.
    """
    logging.info("Initializing storage client and bucket.")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    return storage_client, bucket

def load_model(bucket_name, model_dir):
    """
    Fetch and load the latest model from the bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        model_dir (str): The directory path in the bucket where models are stored.

    Returns:
        tuple: The loaded model, scaler, and PCA.
    """
    logging.info("Fetching and loading the latest model, scaler, and PCA.")
    
    # Fetch latest model, scaler, and pca file names
    latest_model_blob_name = fetch_latest_model(bucket_name, model_dir, 'lstm_model_')
    latest_scaler_blob_name = fetch_latest_model(bucket_name, model_dir, 'scaler_')
    latest_pca_blob_name = fetch_latest_model(bucket_name, model_dir, 'pca_')

    # Download files to local temp directory
    local_model_file_path = '/tmp/model.h5'
    local_scaler_file_path = '/tmp/scaler.pkl'
    local_pca_file_path = '/tmp/pca.pkl'

    download_blob(bucket_name, latest_model_blob_name, local_model_file_path)
    download_blob(bucket_name, latest_scaler_blob_name, local_scaler_file_path)
    download_blob(bucket_name, latest_pca_blob_name, local_pca_file_path)

    # Load the downloaded model, scaler, and PCA
    model = joblib.load(local_model_file_path)
    scaler = joblib.load(local_scaler_file_path)
    pca = joblib.load(local_pca_file_path)

    return model, scaler, pca

def fetch_latest_model(bucket_name, model_dir, prefix):
    """Fetches the latest model file from the specified GCS bucket.
    Args:
        bucket_name (str): The name of the GCS bucket.
        model_dir (str): The directory path in the bucket where models are stored.
        prefix (str): The prefix of the model files in the bucket.
    Returns:
        str: The name of the latest model file.
    """
    logging.info(f"Fetching the latest {prefix}model from bucket: {bucket_name}, directory: {model_dir}")
    blobs = storage_client.list_blobs(bucket_name, prefix=model_dir + prefix)

    blob_names = [blob.name for blob in blobs]
    if not blob_names:
        logging.error(f"Error: No {prefix}model files found in the GCS bucket.")
        raise ValueError(f"No {prefix}model files found in the GCS bucket in directory {model_dir}.")

    latest_blob_name = sorted(blob_names, key=lambda x: x.split('_')[-1], reverse=True)[0]
    return latest_blob_name

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logging.info(f"Blob {source_blob_name} downloaded to {destination_file_name}")

def scale_data(features, scaler, pca):
    try:
        # Convert features to DataFrame
        features_df = pd.DataFrame(features)

        # Log the shape of features_df before scaling
        logging.debug(f"Features DataFrame shape before scaling: {features_df.shape}")

        # Scale the features
        scaled_features = scaler.transform(features_df)

        # Log the shape of scaled_features after scaling
        logging.debug(f"Scaled features shape: {scaled_features.shape}")

        # Apply PCA transformation
        pca_features = pca.transform(scaled_features)

        # Log the shape of pca_features after PCA transformation
        logging.debug(f"PCA features shape: {pca_features.shape}")

        # Reshape for LSTM input
        lstm_features = pca_features.reshape((pca_features.shape[0], pca_features.shape[1], 1))

        # Log the shape of lstm_features after reshaping
        logging.debug(f"LSTM features shape: {lstm_features.shape}")

        return lstm_features
    except Exception as e:
        logging.error(f"Error in scaling data: {e}")
        raise

@app.route(os.environ['AIP_HEALTH_ROUTE'], methods=['GET'])
def health_check():
    """Health check endpoint that returns the status of the server.
    Returns:
        Response: A Flask response with status 200 and "healthy" as the body.
    """
    logging.info("Health check endpoint called.")
    return {"status": "healthy"}

@app.route(os.environ['AIP_PREDICT_ROUTE'], methods=['POST'])
def predict():
    """
    Prediction route that normalizes input data, and returns model predictions.
    Returns:
        Response: A Flask response containing JSON-formatted predictions.
    """
    try:
        logging.info("Prediction route called.")
        data = request.get_json()
        print(data)
        logging.debug(f"Received data: {data}")

        features = data.get('features')
        if features is None:
            raise ValueError("No features provided in the input data")

        if len(features[0]) != 19:
            raise ValueError("Input data must have 19 features.")
        
        # Scaling the features
        scaled_features = scale_data(features, scaler, pca)
        prediction = model.predict(scaled_features)

        logging.info("Prediction complete.")
        return jsonify({'prediction': prediction[0].tolist()})
    except Exception as e:
        logging.error(f"Error in prediction: {e}")
        return jsonify({"error": str(e)}), 500

project_id, bucket_name = initialize_variables()
storage_client, bucket = initialize_client_and_bucket(bucket_name)
model, scaler, pca = load_model(bucket_name, MODEL_DIR)

if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=8080)
    finally:
        # Capture all log messages
        log_messages = logging.getLogger().handlers[1].stream.getvalue()

        # Upload all log messages to GCS
        upload_log_to_gcs(log_messages, gs_log_file_path)