import os
import streamlit as st
import pandas as pd
import numpy as np
from keras.models import load_model
import joblib
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.decomposition import PCA

# Load the model and scaler
PROJECT_DIR = os.environ.get("PROJECT_DIR")
model_path = os.path.join(PROJECT_DIR, 'models', 'lstm_model.h5')
scaler_path = os.path.join(PROJECT_DIR, 'models', 'scaler.pkl')

model = load_model(model_path)
scaler = joblib.load(scaler_path)

# Function to preprocess the input data
def preprocess_data(data):
    # Scale data
    scaled_data = scaler.transform(data)
    # Apply PCA
    pca = PCA(n_components=10)
    pca_data = pca.fit_transform(scaled_data)
    # Reshape for LSTM
    lstm_data = pca_data.reshape((pca_data.shape[0], pca_data.shape[1], 1))
    return lstm_data

# Streamlit app
st.title('GDP Prediction using LSTM')

# Input fields for user data
st.header('Input Data')

country = st.text_input('Country')
feature_1 = st.number_input('Feature 1')
feature_2 = st.number_input('Feature 2')
# Add more input fields as required

input_data = pd.DataFrame({
    'Country': [country],
    'Feature_1': [feature_1],
    'Feature_2': [feature_2],
    # Add more features here
})

# Process input data
clean_data = preprocess_data(input_data)

# Predict button
if st.button('Predict'):
    # Make prediction
    prediction = model.predict(clean_data)
    st.write(f'Predicted GDP: {prediction[0][0]}')

# Save the Streamlit app code into a file
with open('streamlit_app.py', 'w') as f:
    f.write(streamlit_app_code)
