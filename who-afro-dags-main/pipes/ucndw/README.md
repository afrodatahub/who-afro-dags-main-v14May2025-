# UCN Data Warehouse Pipelines

This package includes all data pipelines from extracting data from the WHO AFRO UCN (Communicable and uncommunicable diseases) cluster's Data Warehouse DHIS2 instance and loading that data into the WHO African Regional Data Hub CKAN data catalogue.

## vaccine_coverage pour

The `vaccine_coverage.py` pour is a data pipeline that extracts a DHIS2 Pivot Table of monthly vaccine coverage data from the UCN DW and loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

- "Vaccination Coverage Rates for COUNTRY" contains a single resource with all the vaccination coverage data for that specific country

No transformation is applied to the data, it is loaded as is.

### Work In Progress

Note that we currently don't have a UCN DW access token and so, instead of automatically pulling a Pivot Table from the DHIS2 API, we have manually downloaded this [pivot table](https://ucndw.afro.who.int/dhis/dhis-web-data-visualizer/index.html#/XDZE28VbRr)E as an Excel file and this pipeline looks for that file (named `2024-06-04_vpd.xls`) in the `inputs` directory for this pipline, which is ignored by git.
