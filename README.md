# World Econnomic Growth Forecast

The International Monetary Fund (IMF), since its inception, has played a pivotal role in stabilizing the international landscape of finance and economics. Its comprehensive databases and analytical prowess have been instrumental in aiding member countries to navigate through economic fluctuations and policy-making processes. In the contemporary globalized economy, the IMF's insights are more valuable than ever, providing a wealth of information that can be leveraged to predict future economic trends and potential market shifts.

Building on this foundation, our project aims to harness the vast repository of financial data curated by the IMF to develop robust models for forecasting Gross Domestic Product (GDP) growth across various economies. By employing advanced machine learning techniques and deep learning algorithms, we intend to analyze patterns within the data that could indicate future economic outcomes. This predictive model could serve as a crucial tool for economists, policymakers, and investors alike, offering a glimpse into the economic trajectory of nations in the ever-evolving financial landscape.

## Data Source

### World Economic Outlook 2024 Database
The dataset was obtained from the International Monetary Fund’s website (IMF). The WEO database contains a vast amount of data on various selected macroeconomic indicators for individual countries, regions, and the world as a whole, including national accounts, inflation, unemployment rates, balance of payments, and fiscal indicators.

Source : [Access the dataset here](https://www.imf.org/en/Publications/SPROLLS/world-economic-outlook-databases#sort=%40imfdate%20descending)

### Data Card
- **Size**: 58 columns, 8624 rows
- **File Format**: *.xls
- **Data Format**: Grouped by Subject then Country

### Variables

| Variable Name                | Role      | Type       | Description                                   |
|------------------------------|-----------|------------|-----------------------------------------------|
| WEO Country Code             | ID        | Integer    | Unique code for each country                  |
| WEO Subject Code             | ID        | String     | Code for GDP Parameters                       |
| Country                      | Feature   | String     | Countries of the world (196)                  |
| Subject Descriptor           | Feature   | Categorical| Various Factors affecting GDP                 |
| Units                        | ID        | String     | Unit of GDP Factors                           |
| Scale                        | ID        | String     | Scale of units                                |
| Country/Series-specific notes| ID        | String     | Information about source                      |
| Years (multiple columns)     | Feature   | Continuous | Years considered (1980-2029)                  |
| Estimate Start After         | Feature   | Integer    | Year till which data is collected             |

## Setup Instructions

### Prerequisites

Ensure you have the following installed on your system:

- Docker
- Docker Compose
- Git
- Python 3.x
- Jupyter Notebook 
- DVC 
- TensorFlow 
- MLFlow
- FastAPI/Flask
- Google Cloud Platform

### Step-by-Step Setup

1. **Clone the Repository**
```bash
git clone <repository-url>
cd <repository-directory>
```

#### 2. Initialize DVC

```bash
dvc init
```

#### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

4. Create and Run Docker Containers
```bash
docker-compose up -d
```

5. Run Airflow Scheduler and Webserver
```bash
docker compose up airflow-init
```


