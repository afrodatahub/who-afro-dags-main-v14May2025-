"""
Sandbox script for downloading data from
https://data.who.int/ that can be split by
sex for easy visualization in the AFRO data hub.
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
def gho_oop_dag():
    DATASETS = [
        {
            "file": "OOP_%CHE.csv",
            "title": "Out-of-pocket expenditure as percentage of current health expenditure (CHE) (%)",
            "programme": ["other"],
        },
        {
            "file": "OOP_pc_US$.csv",
            "title": "Out-of-Pocket expenditure (OOP) per capita in US$",
            "programme": ["other"],
        },
    ]

    COUNTRY_SPATIAL_DIM_VALUE_CODE = {
        "GAB": "GABON",
        "BFA": "BURKINA FASO",
        "ZWE": "ZIMBABWE",
        "CMR": "CAMEROON",
        "MRT": "MAURITANIA",
        "SWZ": "ESWATINI",
        "NGA": "NIGERIA",
        "DZA": "ALGERIA",
        "KEN": "KENYA",
        "GHA": "GHANA",
        "ZMB": "ZAMBIA",
        "CPV": "CABO VERDE",
        "LSO": "LESOTHO",
        "ZAF": "SOUTH AFRICA",
        "SYC": "SEYCHELLES",
        "CAF": "CENTRAL AFRICAN REPUBLIC",
        "MUS": "MAURITIUS",
        "SEN": "SENEGAL",
        "MWI": "MALAWI",
        "COD": "DEMOCRATIC REPUBLIC OF CONGO",
        "MOZ": "MOZAMBIQUE",
        "BEN": "BENIN",
        "COG": "REPUBLIC OF CONGO",
        "SSD": "SOUTH SUDAN",
        "ETH": "ETHIOPIA",
        "BDI": "BURUNDI",
        "GIN": "GUINEA",
        "MLI": "MALI",
        "TZA": "UNITED REPUBLIC OF TANZANIA",
        "BWA": "BOTSWANA",
        "CIV": "CÔTE D'IVOIRE",
        "ERI": "ERITREA",
        "GMB": "GAMBIA",
        "MDG": "MADAGASCAR",
        "NAM": "NAMIBIA",
        "NER": "NIGER",
        "RWA": "RWANDA",
        "STP": "SÃO TOMÉ AND PRÍNCIPE",
        "TCD": "CHAD",
        "TGO": "TOGO",
        "UGA": "UGANDA",
        "AGO": "ANGOLA",
        "SLE": "SIERRA LEONE",
        "COM": "COMOROS",
        "GNB": "GUINEA-BISSAU",
        "GNQ": "EQUATORIAL GUINEA",
        "LBR": "LIBERIA",
        "AFR": "African Region",
    }

    def prepare_ckan_dataset(table, spatial_dim_value_code, title, programme):
        country_name = COUNTRY_SPATIAL_DIM_VALUE_CODE[spatial_dim_value_code]
        log.info(f"Transforming data for CKAN - {country_name}")
        iso_country_codes = wrench.get_iso_country_code([country_name])
        indicator_name = title or table["IND_NAME"].iloc[0]
        name = f"{indicator_name} data for {country_name}"
        programme = ["other"]
        groups = [{"name": "indicators"}]
        notes = "Extracted from the WHO Global Health Observatory - https://www.who.int/data/gho/"

        resources = [
            {
                "name": f"{indicator_name}",
                "upload": table.to_csv(index=False),
                "format": "CSV",
            }
        ]

        return dataset.generate_dataset(
            name,
            groups=groups,
            country=iso_country_codes,
            programme=programme,
            private=False,
            notes=notes,
            resources=resources,
            dataset_type="indicator",
            owner_org="who-afro-dak",
        )

    def prepare_ckan_datasets(table_for_all_countries, title, programme=["other"]):
        log.info("Preparing data for CKAN upload as a list of datasets")
        list_of_datasets = []

        log.info("Processing whole African Region")
        table_for_afro = table_for_all_countries[
            table_for_all_countries["SpatialDimValueCode"].astype(str).isin(COUNTRY_SPATIAL_DIM_VALUE_CODE.keys())
        ]
        if len(table_for_afro) > 0:
            list_of_datasets.append(prepare_ckan_dataset(table_for_afro, "AFR", title, programme))
        else:
            log.info("No data found for African Region")

        return list_of_datasets

    def transform_table_to_match_GHDH_indicator_structure(table):
        log.info("Transforming table to match GHDH indicator structure")
        table = table.rename(
            columns={
                "IndicatorCode": "IND_CODE",
                "Indicator": "IND_NAME",
                "Period type": "DIM_TIME_TYPE",
                "Period": "DIM_TIME",
                "Location type": "DIM_GEO_TYPE",
                "Location": "GEO_NAME_SHORT",
                "FactValueNumeric": "PERCENTAGE_N",
            }
        )
        table["GEO_NAME_SHORT"].where(~table["GEO_NAME_SHORT"].str.fullmatch("Africa"), "African Region", inplace=True)
        return table

    @task
    def run():
        list_of_datasets = []
        for dataset_dict in DATASETS:
            log.info(f"Processing {dataset_dict['file']}")
            table = pd.read_csv(
                os.path.join(os.path.dirname(__file__), "inputs", dataset_dict["file"]),
            )

            table = transform_table_to_match_GHDH_indicator_structure(table)

            list_of_datasets.extend(
                prepare_ckan_datasets(table, title=dataset_dict["title"], programme=dataset_dict["programme"])
            )

        ckan.ckan_load(list_of_datasets)

    return run()


gho_oop_pour = gho_oop_dag()

if __name__ == "__main__":
    gho_oop_pour.test()
