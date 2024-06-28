import os
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.decomposition import PCA
from keras.models import Sequential
from keras.layers import LSTM, Dense
import joblib  # For saving the model and scaler
import time
from memory_profiler import memory_usage

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(PROJECT_DIR)

# from utilities.logger import setup_logging
# my_logger = setup_logging()
# my_logger.set_logger("model_training_logger")

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
        # my_logger.write('info', f"Filtered data successfully loaded from {filtered_pickle_path}")
        return df
    except FileNotFoundError:
        # my_logger.write('error', "The filtered pickle file does not exist.")
        raise FileNotFoundError("The filtered pickle file does not exist.")
    except Exception as e:
        # my_logger.write('error', f"Failed to load filtered data: {e}")
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
        # my_logger.write('error', f"Error in dropping columns: {e}")
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
        # my_logger.write('error', f"Error in imputing missing values: {e}")
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
        # my_logger.write('error', f"Error in dropping specific columns: {e}")
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
        # my_logger.write('error', f"Error in cleaning data: {e}")
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
        # my_logger.write('error', f"Error in scaling data: {e}")
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
        # my_logger.write('error', f"Error in running pipeline: {e}")
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
        # my_logger.write('error', f"Error in training model: {e}")
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
        # my_logger.write('error', f"Error in prediction: {e}")
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
        model.save(model_path)
        with open(scaler_path, "wb") as f:
            joblib.dump(scaler, f)
        with open(pca_path, "wb") as f:
            joblib.dump(pca, f)
    except Exception as e:
        # my_logger.write('error', f"Error in saving model and scaler: {e}")
        raise

if __name__ == "__main__":
    try:
        
        # Define the path to the filtered pickle file
        filtered_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'filtered_data.pkl')
        
        df = load_filtered_data(filtered_pickle_path)
        clean_df = clean_data(df)

        X_train_lstm, X_test_lstm, y_train, y_test, scaler, pca = run_pipeline(clean_df)
        model = train_model(X_train_lstm, y_train)
        y_pred, inference_time, max_mem_usage = predict(model, X_test_lstm)

        model_path = os.path.join(PROJECT_DIR, 'models', 'lstm_model.h5')
        scaler_path = os.path.join(PROJECT_DIR, 'models', 'scaler.pkl')
        pca_path = os.path.join(PROJECT_DIR, 'models', 'pca.pkl')
        save_model_and_scaler(model, pca, scaler, model_path, scaler_path, pca_path)

        print(f'Inference Time: {inference_time}')
        print(f'Max Memory Usage: {max_mem_usage}')
    except Exception as e:
        # my_logger.write('error', f"Error in main execution: {e}")
        raise