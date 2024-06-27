from typing import List
from pydantic import BaseModel, Field, ConfigDict

class WeoRawRecord(BaseModel):
    model_config = ConfigDict(extra='forbid')

    weo_country_code: str | float = Field(..., alias='WEO Country Code')
    weo_subject_code: str | float = Field(..., alias='WEO Subject Code')
    country: str | float = Field(..., alias='Country')
    subject_descriptor: str | float = Field(..., alias='Subject Descriptor')
    units: str | float = Field(..., alias='Units')
    scale: str | float = Field(..., alias='Scale')
    country_series_specific_notes: str | float = Field(..., alias='Country/Series-specific Notes')
    year_1980: float = Field(..., alias='1980')
    year_1981: float = Field(..., alias='1981')
    year_1982: float = Field(..., alias='1982')
    year_1983: float = Field(..., alias='1983')
    year_1984: float = Field(..., alias='1984')
    year_1985: float = Field(..., alias='1985')
    year_1986: float = Field(..., alias='1986')
    year_1987: float = Field(..., alias='1987')
    year_1988: float = Field(..., alias='1988')
    year_1989: float = Field(..., alias='1989')
    year_1990: float = Field(..., alias='1990')
    year_1991: float = Field(..., alias='1991')
    year_1992: float = Field(..., alias='1992')
    year_1993: float = Field(..., alias='1993')
    year_1994: float = Field(..., alias='1994')
    year_1995: float = Field(..., alias='1995')
    year_1996: float = Field(..., alias='1996')
    year_1997: float = Field(..., alias='1997')
    year_1998: float = Field(..., alias='1998')
    year_1999: float = Field(..., alias='1999')
    year_2000: float = Field(..., alias='2000')
    year_2001: float = Field(..., alias='2001')
    year_2002: float = Field(..., alias='2002')
    year_2003: float = Field(..., alias='2003')
    year_2004: float = Field(..., alias='2004')
    year_2005: float = Field(..., alias='2005')
    year_2006: float = Field(..., alias='2006')
    year_2007: float = Field(..., alias='2007')
    year_2008: float = Field(..., alias='2008')
    year_2009: float = Field(..., alias='2009')
    year_2010: float = Field(..., alias='2010')
    year_2011: float = Field(..., alias='2011')
    year_2012: float = Field(..., alias='2012')
    year_2013: float = Field(..., alias='2013')
    year_2014: float = Field(..., alias='2014')
    year_2015: float = Field(..., alias='2015')
    year_2016: float = Field(..., alias='2016')
    year_2017: float = Field(..., alias='2017')
    year_2018: float = Field(..., alias='2018')
    year_2019: float = Field(..., alias='2019')
    year_2020: float = Field(..., alias='2020')
    year_2021: float = Field(..., alias='2021')
    year_2022: float = Field(..., alias='2022')
    year_2023: float = Field(..., alias='2023')
    year_2024: float = Field(..., alias='2024')
    year_2025: float = Field(..., alias='2025')
    year_2026: float = Field(..., alias='2026')
    year_2027: float = Field(..., alias='2027')
    year_2028: float = Field(..., alias='2028')
    year_2029: float = Field(..., alias='2029')
    estimates_start_after: str | float = Field(..., alias='Estimates Start After')

class WeoRawContainer(BaseModel):
    records: List[WeoRawRecord]




class WeoCleanRecord(BaseModel):
    pass

class WeoCleanContainer(BaseModel):
    ...