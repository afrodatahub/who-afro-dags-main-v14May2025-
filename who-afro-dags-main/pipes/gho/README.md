# Global Health Observatory Pipeline

This package includes data pipelines for extracting data from the WHO Global Health Observatory (https://www.who.int/data/gho) and loading that data into the WHO Africa Regional Health Data Hub's CKAN data catalogue.

## Oot-of-pocket (OOP) pour

The `oop.py` pour is a data pipeline that two out-of-pocket-related indicators from the GHO.
The pipeline transforms that data by selecting only African-region countries (and the WHO African region as a whole) using the SpatialDimValueCode field.
The data is also transformed so that any columns matching the GHDH indicator schema is renamed for consistency (and so that the dashboards work).
The pipeline then loads it into the CKAN data catalogue as "INDICATOR for African Region", which contains one resource with the indicator measurements for the whole African region.

### Work In Progress

Right now this actually process anything in `inputs/` rather than the GHO API. This is a shortcut until we have built the GHO utils.
The two files in `inputs/` were actually generated by merging the "Country" and "WHO region" location type CSV files that can be downloaded for each indicator here:

- https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us
- https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-as-percentage-of-current-health-expenditure-(che)-(-)
