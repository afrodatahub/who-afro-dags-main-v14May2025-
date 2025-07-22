# World Malaria Report DHIS2 Pipeline

This package includes a data pipeline for extracting data from the WHO World Malaria Report's DHIS2 instance and loading that data into the WHO African Regional Data Hub CKAN data catalogue.

## WMR pour

The `wmr.py` pour is a data pipeline that extracts a DHIS2 Pivot Table of annual calculated malaria variables used in the annual World Malaria Report and loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

- "World Malaria Report Variables for African Region" contains one resource per year (2015--2023) with all the calculated variables for the World Malaria Report for every country in the African region (one row per country).

Data is transformed by splitting it into one resource CSV file per year.

### Work In Progress

Note that we are currently using an Excel file that we have manually downloaded this [pivot table](https://extranet.who.int/dhis2/dhis-web-data-visualizer/index.html#/oZtG1h5jrI4) file and this pipeline looks for that file (named `wmr_2015--2023.xls`) in the `inputs` directory for this pipline. This is a shortcut until we have built the DHIS2 utils.

Because of this, variable names are still in machine readable form.
