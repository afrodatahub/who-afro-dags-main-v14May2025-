# Global Database for Tracking Antimicrobial Resistance (AMR) Country Self- Assessment Survey (TrACSS) Pipeline

This package includes a pipeline for extracting data from TrACSS, filtering by region / country and loading into CKAN.

## traccs pour

The `tracss.py` pour is the main data pipeline that extracts yearly country responses as Excel data from the TrACSS website, transforms the data by filtering by region / country and then loads it into the WHO African Regional Data Hub CKAN data catalogue in the following structures:

- "TrACSS African Region Country Responses" contains a resource per year with all the country responses for the African region as published on TrACSS.
