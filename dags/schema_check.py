# Clean blank rows before schema generation
# Remove float from some columns which are compulsary

import pandas as pd
from pydantic import BaseModel, Field, validator, ValidationError
from datetime import datetime
from typing import Optional, List

def add_year_fields(cls):
    current_year = datetime.now().year
    start_year = 1980
    
    # Add fields for past years
    for year in range(start_year, current_year):
        field_name = f'_{year}'
        cls.__annotations__[field_name] = str | float
        cls.model_fields[field_name] = Field(..., description=f"Data for year {year}")
    
    # Add optional fields for future years
    for year in range(current_year, current_year + 6):
        field_name = f'_{year}'
        cls.__annotations__[field_name] = str | float | None
        cls.model_fields[field_name] = Field(None, description=f"Data for year {year}")
    
    return cls

@add_year_fields
class WeoRawRecord(BaseModel):
    weo_country_code: str | float = Field(..., alias='WEO Country Code')
    weo_subject_code: str | float = Field(..., alias='WEO Subject Code')
    country: str | float = Field(..., alias='Country')
    subject_descriptor: str | float = Field(..., alias='Subject Descriptor')
    units: str | float = Field(..., alias='Units')
    scale: str | float = Field(..., alias='Scale')
    country_series_specific_notes: str | float = Field(..., alias='Country/Series-specific Notes')
    estimates_start_after: str | float = Field(..., alias='Estimates Start After')

class WeoRawContainer(BaseModel):
    records: List[WeoRawRecord]

class WeoTransformed(BaseModel):
    pass


data = pd.read_excel("./data/raw_data/IMF_WEO_Data.xlsx", header=0, dtype=str)
data.columns = map(str, data.columns)

model = WeoRawContainer(records=[WeoRawRecord(**item) for item in data.to_dict(orient='records')[:1]])
print(model.model_dump())
model.model_rebuild()
print(model.model_dump())