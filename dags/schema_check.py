# Clean blank rows before schema generation

import pandas as pd
from pydantic import BaseModel, Field, validator, ValidationError

class WeoRaw(BaseModel):
    weo_country_code: str | None = Field(..., alias='WEO Country Code')
    weo_subject_code: str | None = Field(..., alias='WEO Subject Code')
    country: str | None = Field(..., alias='Country')
    subject_descriptor: str | None = Field(..., alias='Subject Descriptor')
    units: str | None = Field(..., alias='Units')
    scale: str | None = Field(..., alias='Scale')
    country_series_specific_notes: str | None = Field(..., alias='Country/Series-specific Notes')
    estimates_start_after: str | None = Field(..., alias='Estimates Start After')

    # Custom validator to validate year columns
    @validator('*', pre=True)
    def validate_year_columns(cls, v):
        ...

class WeoTransformed(BaseModel):
    pass

data = pd.read_excel("./data/raw_data/IMF_WEO_Data.xlsx", header=0, dtype=str)
data.columns = map(lambda x: str(x), data.columns)
WeoRaw(**data.to_dict())