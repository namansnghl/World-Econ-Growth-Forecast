from io import StringIO
from google.cloud import storage
import os
import logging
import gcsfs
import pandas as pd
from sklearn.preprocessing import RobustScaler, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from keras.models import Sequential
from keras.layers import LSTM, Dense
import joblib 
import time
from memory_profiler import memory_usage

def configure_logging():
    # Configure logging to console and a custom StringIO handler
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    log_stream = StringIO()
    file_handler = logging.StreamHandler(log_stream)
    logging.getLogger().addHandler(file_handler)

configure_logging()

fs = gcsfs.GCSFileSystem(project='wegf-mlops')
storage_client = storage.Client()
bucket_name = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ.get("AIP_STORAGE_URI")

# Configure logging
gs_log_file_path = f'gs://{bucket_name}/logs/flask_log_file.log'

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

def load_filtered_data(filtered_pickle_path):
    """
    Load the filtered data from a pickle file.

    Args:
    filtered_pickle_path (str): Path to the filtered pickle file.

    Returns:
    pandas.DataFrame: Loaded DataFrame.

    Raises:
    FileNotFoundError: If the pickle file does not exist.
    Exception: If loading the data fails.
    """
    try:
        df = pd.read_pickle(filtered_pickle_path)
        logging.write('info', f"Filtered data successfully loaded from {filtered_pickle_path}")
        return df
    except FileNotFoundError:
        logging.write('error', "The filtered pickle file does not exist.")
        raise FileNotFoundError("The filtered pickle file does not exist.")
    except Exception as e:
        logging.write('error', f"Failed to load filtered data: {e}")
        raise

def drop_columns(df):
    """
    Drop specific columns from the DataFrame.

    Args:
    df (pandas.DataFrame): Input DataFrame.

    Returns:
    pandas.DataFrame: DataFrame with specific columns dropped.
    """
    try:
        df.drop(columns=['Unemployment rate - Percent of total labor force (Units)'], inplace=True)
        return df
    except Exception as e:
        logging.write('error', f"Error in dropping columns: {e}")
        raise

def impute_missing_values(df):
    """
    Impute missing values in the DataFrame by filling with the mean of each group.

    Args:
    df (pandas.DataFrame): Input DataFrame with missing values.

    Returns:
    pandas.DataFrame: DataFrame with missing values imputed.
    """
    try:
        def impute_group(group):
            return group.fillna(group.mean())

        grouped_df = df.groupby('Country')
        imputed_df = grouped_df.apply(impute_group)
        imputed_df = imputed_df.reset_index(drop=True)
        return imputed_df
    except Exception as e:
        logging.write('error', f"Error in imputing missing values: {e}")
        raise

def drop_specific_columns(df):
    """
    Drop specific columns from the DataFrame.

    Args:
    df (pandas.DataFrame): Input DataFrame.

    Returns:
    pandas.DataFrame: DataFrame with specific columns dropped.
    """
    try:
        columns_to_drop = [
            'Gross domestic product based on purchasing-power-parity (PPP) share of world total - Percent (Units)',
            'Gross domestic product corresponding to fiscal year, current prices - National currency (Billions)',
            'Gross domestic product per capita, constant prices - National currency (Units)',
            'Gross domestic product per capita, constant prices - Purchasing power parity; 2017 international dollar (Units)',
            'Gross domestic product per capita, current prices - Purchasing power parity; international dollars (Units)',
            'Gross domestic product per capita, current prices - U.S. dollars (Units)',
            'Gross domestic product, constant prices - National currency (Billions)',
            'Gross domestic product, constant prices - Percent change (Units)',
            'Gross domestic product, current prices - National currency (Billions)',
            'Gross domestic product, current prices - Purchasing power parity; international dollars (Billions)',
            'Gross domestic product, current prices - U.S. dollars (Billions)'
        ]
        df.drop(columns=columns_to_drop, inplace=True)
        return df
    except Exception as e:
        logging.write('error', f"Error in dropping specific columns: {e}")
        raise

def clean_data(df):
    """
    Clean the data by dropping unnecessary columns and imputing missing values.

    Args:
    df (pandas.DataFrame): Raw input DataFrame.

    Returns:
    pandas.DataFrame: Cleaned DataFrame.
    """
    try:
        df = drop_columns(df)
        imputed_df = impute_missing_values(df)
        cleaned_df = drop_specific_columns(imputed_df)
        cleaned_df = cleaned_df.dropna()
        return cleaned_df
    except Exception as e:
        logging.write('error', f"Error in cleaning data: {e}")
        raise

def scale_data(df):
    """
    Scale the data using RobustScaler.

    Args:
    df (pandas.DataFrame): Input DataFrame.

    Returns:
    tuple: Scaled DataFrame and the scaler object.
    """
    try:
        RS = RobustScaler()
        scaled_data = RS.fit_transform(df)
        scaled_df = pd.DataFrame(scaled_data, columns=df.columns)
        return scaled_df, RS
    except Exception as e:
        logging.write('error', f"Error in scaling data: {e}")
        raise

def run_pipeline(data):
    """
    Run the preprocessing pipeline including scaling, splitting, and PCA transformation.

    Args:
    data (pandas.DataFrame): Cleaned input DataFrame.

    Returns:
    tuple: Processed training and test data, and scaler object.
    """
    try:
        RS = RobustScaler()
        scale = RS.fit_transform(data)
        data = pd.DataFrame(scale, columns=data.columns)
        target = data['Gross domestic product per capita, current prices - National currency (Units)']
        X = data.drop(columns=['Gross domestic product per capita, current prices - National currency (Units)'])
        y = target

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        pca = PCA(n_components=10)
        X_train_pca = pca.fit_transform(X_train_scaled)
        X_test_pca = pca.transform(X_test_scaled)

        X_train_lstm = X_train_pca.reshape((X_train_pca.shape[0], X_train_pca.shape[1], 1))
        X_test_lstm = X_test_pca.reshape((X_test_pca.shape[0], X_test_pca.shape[1], 1))

        return X_train_lstm, X_test_lstm, y_train, y_test, scaler, pca
    except Exception as e:
        logging.write('error', f"Error in running pipeline: {e}")
        raise

def train_model(X_train_lstm, y_train):
    """
    Train an LSTM model.

    Args:
    X_train_lstm (numpy.ndarray): Training features reshaped for LSTM.
    y_train (numpy.ndarray): Training target values.

    Returns:
    Sequential: Trained LSTM model.
    """
    try:
        model = Sequential()
        model.add(LSTM(50, activation='relu', input_shape=(X_train_lstm.shape[1], 1)))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mse')
        model.fit(X_train_lstm, y_train, validation_split=0.2, epochs=100, batch_size=32, verbose=1)
        return model
    except Exception as e:
        # logging.write('error', f"Error in training model: {e}")
        raise

def predict(model, X_test_lstm):
    """
    Predict using the trained model and measure inference time and memory usage.

    Args:
    model (Sequential): Trained LSTM model.
    X_test_lstm (numpy.ndarray): Test features reshaped for LSTM.

    Returns:
    tuple: Predictions, inference time, and maximum memory usage.
    """
    try:
        start_time = time.time()
        y_pred = model.predict(X_test_lstm)
        end_time = time.time()

        mem_usage = memory_usage((lambda: model.predict(X_test_lstm)), interval=0.1)
        max_mem_usage = max(mem_usage)

        return y_pred, end_time - start_time, max_mem_usage
    except Exception as e:
        logging.write('error', f"Error in prediction: {e}")
        raise

def save_model_and_scaler(model, pca, scaler, model_path, scaler_path, pca_path):
    """
    Save the trained model and scaler.

    Args:
    model (Sequential): Trained LSTM model.
    scaler (RobustScaler): Scaler used for scaling the data.
    model_path (str): Path to save the model.
    scaler_path (str): Path to save the scaler.
    """
    try:
        # Save model to a local temporary file
        local_model_file_path = "model.h5"
        joblib.dump(model, local_model_file_path)

        # Save scaler to a local temporary file
        local_scaler_file_path = "scaler.pkl"
        joblib.dump(scaler, local_scaler_file_path)

        # Save PCA to a local temporary file
        local_pca_file_path = "pca.pkl"
        joblib.dump(pca, local_pca_file_path)

        with fs.open(os.path.join(MODEL_DIR, model_path), 'wb') as f:
            with open(local_model_file_path, 'rb') as local_f:
                f.write(local_f.read())
        
        with fs.open(os.path.join(MODEL_DIR, scaler_path), 'wb') as f:
            with open(local_scaler_file_path, 'rb') as local_f:
                f.write(local_f.read())

        with fs.open(os.path.join(MODEL_DIR, pca_path), 'wb') as f:
            with open(local_pca_file_path, 'rb') as local_f:
                f.write(local_f.read())

        logging.info(f"Model, scaler, and PCA successfully uploaded to {MODEL_DIR}")
    except Exception as e:
        logging.error(f"Error uploading model, scaler, and PCA: {e}")
        raise
    finally:
        # Clean up the temporary files
        for file_path in [local_model_file_path, local_scaler_file_path, local_pca_file_path]:
            if os.path.exists(file_path):
                os.remove(file_path)

if __name__ == "__main__":
    try:
        
        # Define the path to the filtered pickle file
        filtered_pickle_path = f'gs://{bucket_name}/data/processed_data/filtered_data.pkl'
        
        df = load_filtered_data(filtered_pickle_path)
        clean_df = clean_data(df)

        X_train_lstm, X_test_lstm, y_train, y_test, scaler, pca = run_pipeline(clean_df)
        model = train_model(X_train_lstm, y_train)
        y_pred, inference_time, max_mem_usage = predict(model, X_test_lstm)

        model_path = 'lstm_model.h5'
        scaler_path = 'scaler.pkl'
        pca_path = 'pca.pkl'
        save_model_and_scaler(model, pca, scaler, model_path, scaler_path, pca_path)

        print(f'Inference Time: {inference_time}')
        print(f'Max Memory Usage: {max_mem_usage}')
    except Exception as e:
        logging.write('error', f"Error in main execution: {e}")
        raise