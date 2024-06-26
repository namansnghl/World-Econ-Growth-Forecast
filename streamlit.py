import os
import streamlit as st
import pandas as pd
import numpy as np
from keras.models import load_model
import joblib
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.decomposition import PCA

os.environ["PROJECT_DIR"] = "/Users/nisharggosai/Desktop/MLOps Project/World-Econ-Growth-Forecast"
# Set project directory
PROJECT_DIR = os.environ.get("PROJECT_DIR")
model_path = os.path.join(PROJECT_DIR, 'models', 'lstm_model.h5')
scaler_path = os.path.join(PROJECT_DIR, 'models', 'scaler.pkl')

# Load model and scaler
model = load_model(model_path)
scaler = joblib.load(scaler_path)

# Dictionary mapping country codes to country names
country_code_to_name = {
    111: 'United States', 112: 'United Kingdom', 122: 'Austria', 124: 'Belgium', 128: 'Denmark', 
    132: 'France', 134: 'Germany', 136: 'Italy', 137: 'Luxembourg', 138: 'Netherlands', 142: 'Norway', 
    144: 'Sweden', 146: 'Switzerland', 156: 'Canada', 158: 'Japan', 172: 'Finland', 174: 'Greece', 
    176: 'Iceland', 178: 'Ireland', 181: 'Malta', 182: 'Portugal', 184: 'Spain', 186: 'Türkiye', 
    193: 'Australia', 196: 'New Zealand', 199: 'South Africa', 213: 'Argentina', 218: 'Bolivia', 
    223: 'Brazil', 228: 'Chile', 233: 'Colombia', 238: 'Costa Rica', 243: 'Dominican Republic', 
    253: 'El Salvador', 258: 'Guatemala', 263: 'Haiti', 268: 'Honduras', 273: 'Mexico', 
    278: 'Nicaragua', 283: 'Panama', 288: 'Paraguay', 293: 'Peru', 313: 'The Bahamas', 
    316: 'Barbados', 321: 'Dominica', 328: 'Grenada', 336: 'Guyana', 339: 'Belize', 
    343: 'Jamaica', 366: 'Suriname', 369: 'Trinidad and Tobago', 419: 'Bahrain', 423: 'Cyprus', 
    429: 'Islamic Republic of Iran', 436: 'Israel', 439: 'Jordan', 443: 'Kuwait', 446: 'Lebanon', 
    449: 'Oman', 456: 'Saudi Arabia', 463: 'Syria', 466: 'United Arab Emirates', 469: 'Egypt', 
    474: 'Yemen', 512: 'Afghanistan', 513: 'Bangladesh', 514: 'Bhutan', 516: 'Brunei Darussalam', 
    518: 'Myanmar', 522: 'Cambodia', 524: 'Sri Lanka', 528: 'Taiwan Province of China', 532: 'Hong Kong SAR', 
    534: 'India', 536: 'Indonesia', 542: 'Korea', 544: 'Lao P.D.R.', 548: 'Malaysia', 
    556: 'Maldives', 558: 'Nepal', 564: 'Pakistan', 566: 'Philippines', 576: 'Singapore', 
    578: 'Thailand', 582: 'Vietnam', 611: 'Djibouti', 612: 'Algeria', 614: 'Angola', 
    616: 'Botswana', 618: 'Burundi', 622: 'Cameroon', 624: 'Cabo Verde', 626: 'Central African Republic', 
    628: 'Chad', 632: 'Comoros', 636: 'Democratic Republic of the Congo', 638: 'Benin', 642: 'Equatorial Guinea', 
    643: 'Eritrea', 644: 'Ethiopia', 646: 'Gabon', 648: 'The Gambia', 652: 'Ghana', 
    654: 'Guinea-Bissau', 656: 'Guinea', 662: "Côte d'Ivoire", 664: 'Kenya', 666: 'Lesotho', 
    668: 'Liberia', 672: 'Libya', 674: 'Madagascar', 676: 'Malawi', 678: 'Mali', 
    682: 'Mauritania', 684: 'Mauritius', 686: 'Morocco', 688: 'Mozambique', 692: 'Niger', 
    694: 'Nigeria', 714: 'Rwanda', 716: 'São Tomé and Príncipe', 718: 'Seychelles', 722: 'Senegal', 
    724: 'Sierra Leone', 728: 'Namibia', 732: 'Sudan', 734: 'Eswatini', 738: 'Tanzania', 
    742: 'Togo', 744: 'Tunisia', 746: 'Uganda', 748: 'Burkina Faso', 754: 'Zambia', 
    813: 'Solomon Islands', 819: 'Fiji', 846: 'Vanuatu', 862: 'Samoa', 866: 'Tonga', 
    911: 'Armenia', 912: 'Azerbaijan', 913: 'Belarus', 914: 'Albania', 915: 'Georgia', 
    916: 'Kazakhstan', 917: 'Kyrgyz Republic', 918: 'Bulgaria', 921: 'Moldova', 922: 'Russia', 
    923: 'Tajikistan', 924: 'China', 925: 'Turkmenistan', 926: 'Ukraine', 927: 'Uzbekistan', 
    935: 'Czech Republic', 936: 'Slovak Republic', 939: 'Estonia', 941: 'Latvia', 942: 'Serbia', 
    943: 'Montenegro', 944: 'Hungary', 946: 'Lithuania', 948: 'Mongolia', 960: 'Croatia', 
    961: 'Slovenia', 962: 'North Macedonia', 963: 'Bosnia and Herzegovina', 964: 'Poland', 968: 'Romania'
}

# Function to preprocess the input data
def preprocess_data(data, scaler):
    """
    Preprocess user input data to match the format used for prediction.

    Args:
    data (pd.DataFrame): User input data.
    scaler: Scaler object used for scaling the data during training.

    Returns:
    np.ndarray: Preprocessed data reshaped for LSTM model input.
    """
    try:
        # Convert all inputs to float64 to ensure consistency
        numerical_data = data.astype(np.float64)
        
        # Scale numerical features using the loaded scaler
        scaled_data = scaler.transform(numerical_data)

        # Prepare data for LSTM model input (reshape to (samples, timesteps, features))
        lstm_data = scaled_data.reshape((scaled_data.shape[0], scaled_data.shape[1], 1))

        return lstm_data
    except Exception as e:
        st.error(f"Error during preprocessing: {e}")

# Function to display country name based on country code
def get_country_name(country_code):
    if country_code in country_code_to_name:
        return country_code_to_name[country_code]
    else:
        return "Country name not found"

# Streamlit app
st.title('Gross domestic product prediction')

# Input fields for user data
st.header('Please provide the following information')

# Initialize dictionary to store user inputs
user_inputs = {}

# List of features for user input
features = ['Country', 'Year',
            'Current account balance - U.S. dollars (Billions)',
            'General government gross debt - National currency (Billions)',
            'General government net lending/borrowing - National currency (Billions)',
            'General government primary net lending/borrowing - National currency (Billions)',
            'General government revenue - National currency (Billions)',
            'General government total expenditure - National currency (Billions)',
            'Gross domestic product, deflator - Index (Units)',
            'Implied PPP conversion rate - National currency per current international dollar (Units)',
            'Inflation, average consumer prices - Index (Units)',
            'Inflation, average consumer prices - Percent change (Units)',
            'Inflation, end of period consumer prices - Index (Units)',
            'Inflation, end of period consumer prices - Percent change (Units)',
            'Population - Persons (Millions)',
            'Volume of Imports of goods - Percent change (Units)',
            'Volume of exports of goods - Percent change (Units)',
            'Volume of exports of goods and services - Percent change (Units)',
            'Volume of imports of goods and services - Percent change (Units)'
            ]

# Dropdown list of countries for user input
selected_country = st.selectbox('Country', sorted(country_code_to_name.values()))

# Get the WEO Country Code based on selected country
weo_country_code = next(key for key, value in country_code_to_name.items() if value == selected_country)

# Populate user inputs dictionary
user_inputs['WEO Country Code'] = weo_country_code

# Loop through the rest of the features for user input
for feature in features[1:]:  # Skip 'Country' as it's already added
    if feature == 'Year':
        user_inputs[feature] = st.text_input(feature)
    else:
        user_inputs[feature] = st.number_input(feature)

# Create a DataFrame with user input
input_data = pd.DataFrame([user_inputs])

# Display user inputs
st.subheader('User Inputs')
st.write(input_data)

# Get country name based on user input country code
country_code = input_data.iloc[0]['WEO Country Code']
country_name = get_country_name(int(country_code))  # Ensure country_code is converted to int

# # Display predicted GDP for the selected country
# st.subheader(f'Predicted GDP for {country_name}')

# Process input data for prediction
if not input_data.empty:
    try:
        clean_data = preprocess_data(input_data, scaler)
        # Predict button
        if st.button('Predict'):
            # Make prediction using the loaded model
            prediction = model.predict(clean_data)
            st.write(f'Predicted GDP: {prediction[0][0]}')
    except Exception as e:
        st.error(f"Error during input processing: {e}")

# ###################
# Index(['WEO Country Code', 'Year',
#        'Current account balance - U.S. dollars (Billions)',
#        'General government gross debt - National currency (Billions)',
#        'General government net lending/borrowing - National currency (Billions)',
#        'General government primary net lending/borrowing - National currency (Billions)',
#        'General government revenue - National currency (Billions)',
#        'General government total expenditure - National currency (Billions)',,
#        'Gross domestic product, deflator - Index (Units)',
#        'Implied PPP conversion rate - National currency per current international dollar (Units)',
#        'Inflation, average consumer prices - Index (Units)',
#        'Inflation, average consumer prices - Percent change (Units)',
#        'Inflation, end of period consumer prices - Index (Units)',
#        'Inflation, end of period consumer prices - Percent change (Units)',
#        'Population - Persons (Millions)',
#        'Volume of Imports of goods - Percent change (Units)',
#        'Volume of exports of goods - Percent change (Units)',
#        'Volume of exports of goods and services - Percent change (Units)',
#        'Volume of imports of goods and services - Percent change (Units)'],
#       dtype='object', name='Subject')

# [[-0.07751127]
#  [-0.46030302]
#  [-0.19378911]
#  [-0.16471683]
#  [ 0.28191482]
#  [ 0.55062992]
#  [ 0.53987398]
#  [ 0.26616762]
#  [-1.05652391]
#  [-0.5870985 ]]
