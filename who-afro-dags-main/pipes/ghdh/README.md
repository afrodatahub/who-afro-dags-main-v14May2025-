# Global Health Data Hub Pipeline

This package includes a data pipeline for extracting data from the WHO Global Health Data Hub (https://data.who.int/) and loading that data into the WHO African Regional Data Hub CKAN data catalogue.

## Sex pour

The `sex.py` pour is a data pipeline that extracts all indicators in the GHDH that have a DIM_SEX field, transforms that data by selecting only African-region countries using the DIM_GEO_CODE_M49 field and loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

- "INDICATOR for African Region" contains one resource with the indicator measurements for the whole African region.
- "INDICATOR for COUNTRY" contains one resource with all the indicator measurements for that specific country.

Data is transformed by splitting it into one resource CSV file per year or into specific geographic regions.

Note - this pipeline takes 90 minutes to run.

### Work In Progress

Right now this actually process anything in "inputs" rather than the GHDH API. This is a shortcut until we have built the GHDH utils.
