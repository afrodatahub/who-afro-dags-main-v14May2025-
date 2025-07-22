"""
Sandbox script for converting IDSR across WHO AFRO
(2019 -- 2024; monthly) XLS to CSVs for each country and
uploading them to a CKAN instance
"""

import os
from pipes.util import ckan, dataset, wrench
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
def idsr_dag():
    PIVOT_TABLE = "idsr_2019--2024.xls"  # TODO replace with real link to DHIS2

    COUNTRY_KEYS = [
        "GABON",
        "BURKINA FASO",
        "ZIMBABWE",
        "CAMEROON",
        "MAURITANIA",
        "ESWATINI",
        "NIGERIA",
        "ALGERIA",
        "KENYA",
        "GHANA",
        "ZAMBIA",
        "CABO VERDE",
        "LESOTHO",
        "SOUTH AFRICA",
        "SEYCHELLES",
        "CENTRAL AFRICAN REPUBLIC",
        "MAURITIUS",
        "SENEGAL",
        "MALAWI",
        "DEMOCRATIC REPUBLIC OF CONGO",
        "MOZAMBIQUE",
        "BENIN",
        "REPUBLIC OF CONGO",
        "SOUTH SUDAN",
        "ETHIOPIA",
        "BURUNDI",
        "GUINEA",
        "MALI",
        "UNITED REPUBLIC OF TANZANIA",
        "BOTSWANA",
        "CÔTE D'IVOIRE",
        "ERITREA",
        "GAMBIA",
        "MADAGASCAR",
        "NAMIBIA",
        "NIGER",
        "RWANDA",
        "SÃO TOMÉ AND PRÍNCIPE",
        "CHAD",
        "TOGO",
        "UGANDA",
        "ANGOLA",
        "SIERRA LEONE",
        "COMOROS",
        "GUINEA-BISSAU",
        "EQUATORIAL GUINEA",
        "LIBERIA",
        "Africa Region",
    ]

    def format_country_name(key):
        exceptions = {
            "CÔTE D'IVOIRE": "Côte d'Ivoire",
            "DEMOCRATIC REPUBLIC OF CONGO": "Democratic Republic of Congo",
            "CENTRAL AFRICAN REPUBLIC": "Central African Republic",
            "UNITED REPUBLIC OF TANZANIA": "United Republic of Tanzania",
            "REPUBLIC OF CONGO": "Republic of Congo",
            "SÃO TOMÉ AND PRÍNCIPE": "São Tomé and Príncipe",
            "AFRO Region": "AFRO Region",
        }
        return exceptions.get(key, key.title().replace(" Of ", " of ").replace(" And ", " and "))

    def prepare_ckan_dataset(country):
        country_name = country["name"]
        log.info(f"Transforming data for CKAN - {country_name}")
        tags_list = [{"name": "monthly"}]
        iso_country_codes = wrench.get_iso_country_code([country_name])
        name = f"IDSR data for {country_name}"
        programme = ["idsr"]
        notes = (
            "All data elements from the WHO AFRO IDSR DHIS2 instance "
            "for WHO African Region member countries between 2019--2024."
        )

        resources = [
            {
                "name": name,
                "upload": country["df"].to_csv(index=False),
                "format": "CSV",
            }
        ]

        return dataset.generate_dataset(
            name,
            tags=tags_list,
            country=iso_country_codes,
            programme=programme,
            private=True,
            notes=notes,
            resources=resources,
            owner_org="who-afro",  # TODO -vpd
        )

    def prepare_ckan_datasets(topics_as_countries):
        log.info("Preparing data for CKAN upload as a list of datasets")
        list_of_datasets = []
        for topic in topics_as_countries:
            list_of_datasets.append(prepare_ckan_dataset(topic))

        return list_of_datasets

    def extract_pivot_table():
        log.info("Extracting vaccine coverage data from UCN DW")
        log.warn("Using exported Excel file")

        # TODO this next line will get replaced when we have a DHSI2 PAT
        excel_filepath = os.path.join(os.path.dirname(__file__), "inputs", PIVOT_TABLE)
        all_afro_pivot_table = pd.read_excel(
            excel_filepath,
            sheet_name="Sheet 1",
            header=1,
        )
        # replace the column name 'organisationunitname' with 'Country'
        all_afro_pivot_table.rename(columns={"organisationunitname": "Country"}, inplace=True)
        # split the column name 'periodname' into 'Year' and 'Month'
        all_afro_pivot_table.insert(0, "Year", all_afro_pivot_table["periodname"].str.split(" ", expand=True)[1])
        all_afro_pivot_table.insert(1, "Month", all_afro_pivot_table["periodname"].str.split(" ", expand=True)[0])
        all_afro_pivot_table.drop(columns=["periodname"], inplace=True)
        return all_afro_pivot_table

    def transform_topics_as_countries(pivot_table):
        topics_as_countries = []
        for country_key in COUNTRY_KEYS:
            formatted_country_name = format_country_name(country_key)
            topic = {"name": formatted_country_name}
            topic["tags"] = ["idsr", "monthly"]
            topics_as_countries.append(topic)

        log.info("Preparing data subsets for countries")
        for topic in topics_as_countries:
            log.info(f"Getting subdataframe for country {topic['name']}")
            result_df = pivot_table[pivot_table["Country"].str.fullmatch(topic["name"])]
            topic["df"] = result_df
        return topics_as_countries

    @task
    def run():
        pivot_table = extract_pivot_table()

        topics_as_countries = transform_topics_as_countries(pivot_table)

        list_of_datasets = prepare_ckan_datasets(topics_as_countries)

        ckan.ckan_load(list_of_datasets)

    return run()


idsr_pour = idsr_dag()

if __name__ == "__main__":
    idsr_pour.test()
