# iAHO Data Pipelines

This package includes all data pipelines from extracting data from the Integrated African Health Observatory (iAHO) and loading that data into the WHO African Regional Data Hub CKAN data catalogue.

## iAHO pour

The `iaho.py` pour is a data pipeline that extracts all indicator data from the iAHO and loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

* "iAHO data: AFRO region" contains a single resource will all the iAHO indicator data
* "iAHO data: COUNTRY" contains a single resource with all the iAHO indicator data for a specific country
* "iAHO data: PROGRAMME", e.g. "TB", contains multiple resources with all the iAHO indicator data for a specific programme; each resource represents on country

No transformation is applied to the data, it is loaded as is.
