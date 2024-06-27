import os
import streamlit as st
import pandas as pd
import numpy as np
import requests

# Set project directory
PROJECT_DIR = os.environ.get("PROJECT_DIR")

clean_data_path = os.path.join(PROJECT_DIR, 'data', 'processed_data', 'clean_data.pkl')
df = pd.read_pickle(clean_data_path)

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

def get_country_name(country_code):
    if country_code in country_code_to_name:
        return country_code_to_name[country_code]
    else:
        return "Country name not found"

st.title('Gross domestic product prediction')

st.header('Please provide the following information')

user_inputs = {}

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

selected_country = st.selectbox('Country', sorted(country_code_to_name.values()))

weo_country_code = next(key for key, value in country_code_to_name.items() if value == selected_country)

user_inputs['WEO Country Code'] = weo_country_code

years = list(range(1994, 2030))
selected_year = st.selectbox('Year', years)
user_inputs['Year'] = selected_year

country_data = df[df['WEO Country Code'] == weo_country_code]

if not country_data.empty:
    min_values = country_data.min()
    max_values = country_data.max()
else:
    st.error(f"No data available for the selected country: {selected_country}")


for feature in features[2:]:
    min_val = min_values[feature]
    max_val = max_values[feature]
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.write(f"{feature} (Min: {min_val}, Max: {max_val})")
    with col2:
        user_inputs[feature] = st.number_input(
            "", min_value=float(min_val), max_value=float(max_val), value=float(max_val),
            help=f"Please enter a value between {min_val} and {max_val}",
            key=feature
        )


input_data = pd.DataFrame([user_inputs])

st.subheader('User Inputs')
st.write(input_data)


country_code = input_data.iloc[0]['WEO Country Code']
country_name = get_country_name(int(country_code))  


st.subheader(f'Predicted GDP for {country_name}')


def call_prediction_api(input_data):
    api_url = "http://localhost:5000/predict"  
    data = {"features": input_data.values.tolist()}
    response = requests.post(api_url, json=data)
    return response.json()


if not input_data.empty:
    try:
        
        if st.button('Predict'):
            
            result = call_prediction_api(input_data)
            
            if "error" in result:
                st.error(f"Error from API: {result['error']}")
            else:
                st.write(f'Predicted GDP: {result["prediction"]}')
    except Exception as e:
        st.error(f"Error during input processing: {e}")
