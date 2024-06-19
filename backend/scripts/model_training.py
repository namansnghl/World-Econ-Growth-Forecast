import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor
from utilities.logger import setup_logging
import joblib  # For saving the model and scaler

# Set up logging
PROJECT_DIR = os.environ.get("PROJECT_DIR")
my_logger = setup_logging()
my_logger.set_logger("model_training_logger")

def load_filtered_data(filtered_pickle_path):
    """Load the filtered data from a pickle file."""
    try:
        df = pd.read_pickle(filtered_pickle_path)
        my_logger.write('info', f"Filtered data successfully loaded from {filtered_pickle_path}")
        return df
    except FileNotFoundError:
        my_logger.write('error', "The filtered pickle file does not exist.")
        raise FileNotFoundError("The filtered pickle file does not exist.")
    except Exception as e:
        my_logger.write('error', f"Failed to load filtered data: {e}")
        raise

def drop_columns(df):
    df.drop(columns=['Unemployment rate - Percent of total labor force (Units)'], inplace=True)
    return df

def impute_missing_values(df):
    def impute_group(group):
        return group.fillna(group.mean())

    grouped_df = df.groupby('Country')
    imputed_df = grouped_df.apply(impute_group)
    imputed_df = imputed_df.reset_index(drop=True)
    return imputed_df

def drop_specific_columns(df):
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

def clean_data(df):
    df = drop_columns(df)
    imputed_df = impute_missing_values(df)
    cleaned_df = drop_specific_columns(imputed_df)
    cleaned_df = cleaned_df.dropna()
    return cleaned_df

def scale_data(df):
    RS = RobustScaler()
    scaled_data = RS.fit_transform(df)
    scaled_df = pd.DataFrame(scaled_data, columns=df.columns)
    return scaled_df, RS

def train_model(X_train, y_train, X_test, y_test, model_type="xgboost"):
    """Train a machine learning model and evaluate it.
    
    Parameters:
    - X_train: Training features
    - y_train: Training labels
    - X_test: Testing features
    - y_test: Testing labels
    - model_type: Type of model to train ("xgboost" or "random_forest")
    
    Returns:
    - model: Trained model
    """

    if model_type == "xgboost":
        model = XGBRegressor()
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        my_logger.write('info', "XGBoost model training completed.")
        my_logger.write('info', f"Mean Squared Error (MSE) for XGBoost: {mse}")
        my_logger.write('info', f"Mean Absolute Error (MAE) for XGBoost: {mae}")
        my_logger.write('info', f"R-squared (R2) Score for XGBoost: {r2}")

    else:
        raise ValueError("Unsupported model_type. Use 'xgboost' or 'random_forest'.")

    return model

if __name__ == "__main__":
    # Define the path to the filtered pickle file
    filtered_pickle_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'filtered_data.pkl')

    # Load and preprocess data
    df = load_filtered_data(filtered_pickle_path)
    cleaned_df = clean_data(df)
    cleaned_df, scaler = scale_data(cleaned_df)

    target_gdp_column = 'Gross domestic product per capita, current prices - National currency (Units)'
    X = cleaned_df.drop(columns=[target_gdp_column])
    y = cleaned_df[target_gdp_column]
    
    train_size = 0.8
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=1-train_size, random_state=42)

    # Train the model
    model = train_model(X_train, y_train, X_test, y_test, model_type="xgboost")
    # model = train_model(X_train, y_train, X_test, y_test, model_type="random_forest")

    # Save the model and scaler
    model_path = os.path.join(PROJECT_DIR, 'models', 'xgboost_model.pkl')
    scaler_path = os.path.join(PROJECT_DIR, 'models', 'scaler.pkl')
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    my_logger.write('info', f"Model saved to {model_path}")
    my_logger.write('info', f"Scaler saved to {scaler_path}")
