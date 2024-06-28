from flask import Flask, jsonify, request
# from google.cloud import storage
import joblib
import os
import logging
from logging.handlers import RotatingFileHandler
# from dotenv import load_dotenv
from io import StringIO
# import gcsfs
import pandas as pd
from keras.models import load_model

# dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
# load_dotenv(dotenv_path)

current_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(current_dir, 'flask_app.log')

app = Flask(__name__)

# Configure the logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

PROJECT_DIR = os.environ.get("PROJECT_DIR")

# Configure logging
gs_log_file_path = 'gs://us-east1-composer-airflow-7e8e089d-bucket/logs/flask_log_file.log'

# def configure_logging():
#     # Configure logging to console and a custom StringIO handler
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#     log_stream = StringIO()
#     file_handler = logging.StreamHandler(log_stream)
#     logging.getLogger().addHandler(file_handler)

# configure_logging()

# fs = gcsfs.GCSFileSystem(project='wegf-mlops')

# def upload_log_to_gcs(log_messages, gcs_log_path):
#     """
#     Upload log messages to Google Cloud Storage.

#     Args:
#         log_messages (str): The log messages to be uploaded.
#         gcs_log_path (str): The GCS path to upload the log messages.

#     Returns:
#         None
#     """
#     try:
#         with fs.open(gcs_log_path, 'wb') as f:
#             f.write(log_messages.encode())
#         logging.info(f"Log messages successfully uploaded to {gcs_log_path}")
#     except Exception as e:
#         logging.error(f"Error uploading log messages to GCS: {e}")
#         raise


# def initialize_variables():
#     """
#     Initialize environment variables.
#     Returns:
#         tuple: The project id and bucket name.
#     """
#     # Initialize environment variables.
#     logging.info("Initializing environment variables.")
#     project_id = os.getenv("PROJECT_ID")
#     bucket_name = os.getenv("BUCKET_NAME")
#     return project_id, bucket_name

# def initialize_client_and_bucket(bucket_name):
#     """
#     Initialize a storage client and get a bucket object.
#     Args:
#         bucket_name (str): The name of the bucket.
#     Returns:
#         tuple: The storage client and bucket object.
#     """
#     logging.info("Initializing storage client and bucket.")
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket(bucket_name)
#     return storage_client, bucket

# def load_model(bucket, bucket_name):
#     """
#     Fetch and load the latest model from the bucket.
#     Args:
#         bucket (Bucket): The bucket object.
#         bucket_name (str): The name of the bucket.
#     Returns:
#         _BaseEstimator: The loaded model.
#     """
#     # logging.info("Fetching and loading the latest model.")
#     latest_model_blob_name = fetch_latest_model(bucket_name)
#     local_model_file_name = os.path.basename(latest_model_blob_name)
#     model_blob = bucket.blob(latest_model_blob_name)
#     model_blob.download_to_filename(local_model_file_name)
#     model = joblib.load(local_model_file_name)
#     return model

# def fetch_latest_model(bucket_name, prefix="model/model_"):
#     """Fetches the latest model file from the specified GCS bucket.
#     Args:
#         bucket_name (str): The name of the GCS bucket.
#         prefix (str): The prefix of the model files in the bucket.
#     Returns:
#         str: The name of the latest model file.
#     """
#     # logging.info(f"Fetching the latest model from bucket: {bucket_name}")
#     blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

#     blob_names = [blob.name for blob in blobs]
#     if not blob_names:
#         logging.error("Error: No model files found in the GCS bucket.")
#         raise ValueError("No model files found in the GCS bucket.")

#     latest_blob_name = sorted(blob_names, key=lambda x: x.split('_')[-1],
#                               reverse=True)[0]
#     return latest_blob_name

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

# Define a health check route
@app.route('/', methods=['GET'])
def home():
    """Home endpoint that returns a simple message."""
    return "Welcome to the Flask API!"

@app.route('/ping', methods=['GET'])
def health_check():
    """Health check endpoint that returns the status of the server.
    Returns:
        Response: A Flask response with status 200 and "healthy" as the body.
    """
    logging.info("Health check endpoint called.")
    return {"status": "healthy"}

@app.route('/predict', methods=['POST'])
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
# project_id, bucket_name = initialize_variables()
# storage_client, bucket = initialize_client_and_bucket(bucket_name)
# model = load_model(bucket, bucket_name)
# Determine the path to scaler.pkl relative to predict.py
current_dir = os.path.dirname(os.path.abspath(__file__))
scaler_path = os.path.join(current_dir, '..', '..', 'models', 'scaler.pkl')
scaler = joblib.load(scaler_path)
model_path = os.path.join(current_dir, '..', '..', 'models', 'lstm_model.h5') 
model = load_model(model_path)
pca_path = os.path.join(current_dir, '..', '..', 'models', 'pca.pkl') 
pca = joblib.load(pca_path)

if __name__ == '__main__':
    # try:
        app.run(host='0.0.0.0', port=8080)
    # finally:
    #     # Capture all log messages
    #     # log_messages = logging.getLogger().handlers[1].stream.getvalue()

    #     # Upload all log messages to GCS
    #     # upload_log_to_gcs(log_messages, gs_log_file_path)