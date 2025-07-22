"""
sanbox script for converting VPD - Vaccination coverage across WHO AFRO
(2015 -- 2024; monthly) XLS to CSVs for each country and
uploading them to a CKAN instance
"""

import os
from pipes.util import ckan, dataset
from pipes.util.load_dotenv import load_local_dotenv
from pipes.util.logger import log

import pandas as pd
import pendulum
from airflow.decorators import dag, task

load_local_dotenv()


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@daily",
    catchup=False,
)
def wmr_dag():
    PIVOT_TABLE = "wmr_2015--2024.xls"  # TODO replace with real link to DHIS2

    def transform(df):
        log.info("Transforming data for CKAN")
        tags_list = [{"name": "malaria"}, {"name": "yearly"}]
        name = "World Malaria Report Variables for WHO African Region Member Countries"
        programme = ["malaria"]
        notes = (
            "All variables as calculated and used for the annual WHO World Malaria Report "
            "for WHO African Region member countries between 2015--2024."
        )

        resources = []
        for year in range(2015, 2024):
            resources.append(
                {
                    "name": f"WMR {year}",
                    "upload": df[df["Year"] == year].to_csv(index=False),
                    "format": "CSV",
                }
            )

        return dataset.generate_dataset(
            name,
            tags=tags_list,
            programme=programme,
            private=False,
            notes=notes,
            resources=resources,
            owner_org="who-afro-malaria",
        )

    def extract_pivot_table():
        log.info("Extracting vaccine coverage data from UCN DW")
        log.warn("Using exported Excel file")
        # TODO this next line will get replaced when we have a DHSI2 PAT
        excel_filepath = os.path.join(os.path.dirname(__file__), "inputs", PIVOT_TABLE)
        all_years_pivot_table = pd.read_excel(
            excel_filepath,
            sheet_name="Sheet 1",
            header=1,
        )
        all_years_pivot_table.rename(columns={"periodname": "Year"}, inplace=True)
        all_years_pivot_table.rename(columns={"organisationunitname": "Country"}, inplace=True)
        return all_years_pivot_table

    @task
    def run():
        mega_df = extract_pivot_table()

        list_of_datasets = [transform(mega_df)]

        ckan.ckan_load(list_of_datasets)

    return run()


wmr_pour = wmr_dag()

if __name__ == "__main__":
    wmr_pour.test()
