# WHO AFRO IDSR DHIS2 Pipeline

This package includes a data pipeline for extracting data from the WHO African Regional Office Integrated Disease Surveillance and Response (IDSR) DHIS2 instance and loading that data into the WHO African Regional Data Hub CKAN data catalogue.

## IDSR pour

The `idsr.py` pour is a data pipeline that extracts a DHIS2 Pivot Table of monthly IDSR variables for various dieases and loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

- "IDSR Variables for African Region" contains one resource per year (2019--2024) with all the measured variables for the whole African region (one row per month).
- "IDSR Variables for COUNTRY" contains one resource per year (2019--2024) with all the measured variables for that specific country (one row per month).

Data is transformed by splitting it into one resource CSV file per year or into specific geographic regions.

### Work In Progress

Note that we are currently using an Excel file that we have manually downloaded this [pivot table](http://idsr.afro.who.int/dhis-web-data-visualizer/index.html#/G0ZD9mE6bTP) file and this pipeline looks for that file (named `idsr_2019--2024.xls`) in the `inputs` directory for this pipline. This is a shortcut until we have built the DHIS2 utils.
