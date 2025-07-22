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
def ghdh_sex_dag():
    DATASETS = [
        {
            "file": "E2FC6D7_ALL_LATEST.csv",
            "title": (
                "Age-standardized mortality rate attributed to household and ambient air pollution "
                "(per 100 000 population)"
            ),
            "programme": ["other"],
        },
        {
            "file": "2322814_ALL_LATEST.csv",
            "title": "Under-five mortality rate (per 1000 live births)",
            "programme": ["other"],
        },
        {
            "file": "EE6F72A_ALL_LATEST.csv",
            "title": "Total alcohol per capita (>= 15 years of age) consumption (litres of pure alcohol)",
            "programme": ["other"],
        },
        {
            "file": "16BBF41_ALL_LATEST.csv",
            "title": "Suicide mortality rate (per 100 000 population)",
            "programme": ["other"],
        },
        {
            "file": "1F96863_ALL_LATEST.csv",
            "title": "Probability of premature mortality from NCDs (%)",
            "programme": ["ncd"],
        },
        {
            "file": "84FD3DE_ALL_LATEST.csv",
            "title": "Mortality rate from unintentional poisoning (per 100 000 population)",
            "programme": ["other"],
        },
        {
            "file": "361734E_ALL_LATEST.csv",
            "title": "Mortality rate due to homicide (per 100 000 population)",
            "programme": ["vid"],
        },
        {
            "file": "ED50112_ALL_LATEST.csv",
            "title": "Mortality rate attributed to exposure to unsafe WASH services (per 100 000 population)",
            "programme": ["wash"],
        },
        {
            "file": "90E2E48_ALL_LATEST.csv",
            "title": "Life expectancy at birth (years)",
            "programme": ["other"],
        },
        {
            "file": "77D059C_ALL_LATEST.csv",
            "title": "New HIV infections (per 1000 uninfected population)",
            "programme": ["hiv"],
        },
        {
            "file": "75DDA77_ALL_LATEST.csv",
            "title": "Age-standardized prevalence of tobacco use among persons 15 years and older (%)",
            "programme": ["tobacco"],
        },
        {
            "file": "BEFA58B_ALL_LATEST.csv",
            "title": "Age-standardized prevalence of obesity among adults (18+ years)",
            "programme": ["other"],
        },
        {
            "file": "608DE39_ALL_LATEST.csv",
            "title": "Age-standardized prevalence of hypertension among adults aged 30 to 79 years (%)",
            "programme": ["blood-pressure"],
        },
    ]

    COUNTRY_M49s = {
        "266": "GABON",
        "854": "BURKINA FASO",
        "716": "ZIMBABWE",
        "120": "CAMEROON",
        "478": "MAURITANIA",
        "748": "ESWATINI",
        "566": "NIGERIA",
        "12": "ALGERIA",
        "404": "KENYA",
        "288": "GHANA",
        "894": "ZAMBIA",
        "132": "CABO VERDE",
        "426": "LESOTHO",
        "710": "SOUTH AFRICA",
        "690": "SEYCHELLES",
        "140": "CENTRAL AFRICAN REPUBLIC",
        "480": "MAURITIUS",
        "686": "SENEGAL",
        "454": "MALAWI",
        "180": "DEMOCRATIC REPUBLIC OF CONGO",
        "508": "MOZAMBIQUE",
        "204": "BENIN",
        "178": "REPUBLIC OF CONGO",
        "728": "SOUTH SUDAN",
        "231": "ETHIOPIA",
        "108": "BURUNDI",
        "324": "GUINEA",
        "466": "MALI",
        "834": "UNITED REPUBLIC OF TANZANIA",
        "72": "BOTSWANA",
        "384": "CÔTE D'IVOIRE",
        "232": "ERITREA",
        "270": "GAMBIA",
        "450": "MADAGASCAR",
        "516": "NAMIBIA",
        "562": "NIGER",
        "646": "RWANDA",
        "678": "SÃO TOMÉ AND PRÍNCIPE",
        "148": "CHAD",
        "768": "TOGO",
        "800": "UGANDA",
        "24": "ANGOLA",
        "694": "SIERRA LEONE",
        "174": "COMOROS",
        "624": "GUINEA-BISSAU",
        "226": "EQUATORIAL GUINEA",
        "430": "LIBERIA",
        "953": "AFRO",
        "AFRO": "AFRO",
    }

    def prepare_ckan_dataset(table, m49, title, programme):
        country_name = COUNTRY_M49s[m49]
        country_name = "AFRO" if country_name == "AFRO" else country_name.title()
        log.info(f"Transforming data for CKAN - {country_name}")
        iso_country_codes = wrench.get_iso_country_code([country_name])
        indicator_name = title or table["IND_NAME"].iloc[0]
        name = f"{indicator_name} data for {country_name}"
        programme = ["other"]
        groups = [{"name": "indicators"}]
        notes = "Extracted from the Global Health Data Hub - https://data.who.int/"

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
        # for country in COUNTRY_M49s.keys():
        #     country_name = COUNTRY_M49s[country]
        #     log.info(f"Processing {country_name}")
        #     table_for_this_country = table_for_all_countries[
        #         table_for_all_countries["DIM_GEO_CODE_M49"].astype(str).str.fullmatch(country)
        #     ]
        #     if len(table_for_this_country) > 0:
        #         list_of_datasets.append(prepare_ckan_dataset(table_for_this_country, country))
        #     else:
        #         log.info(f"No data found for {country_name}")

        # all AFRO
        log.info("Processing whole African Region")
        table_for_afro = table_for_all_countries[
            table_for_all_countries["DIM_GEO_CODE_M49"].astype(str).isin(COUNTRY_M49s.keys())
        ]
        table_for_afro["GEO_NAME_SHORT"][
            table_for_afro["DIM_GEO_CODE_M49"].astype(str).str.fullmatch("953")
        ] = "African Region"
        if len(table_for_afro) > 0:
            list_of_datasets.append(prepare_ckan_dataset(table_for_afro, "AFRO", title, programme))
        else:
            log.info("No data found for African Region")

        return list_of_datasets

    @task
    def run():
        list_of_datasets = []
        for dataset_dict in DATASETS:
            log.info(f"Processing {dataset_dict['file']}")
            table = pd.read_csv(
                os.path.join(os.path.dirname(__file__), "inputs", dataset_dict["file"]),
            )

            log.info("Checking has field DIM_SEX")
            if "DIM_SEX" in table.columns:
                list_of_datasets.extend(
                    prepare_ckan_datasets(table, title=dataset_dict["title"], programme=dataset_dict["programme"])
                )

        ckan.ckan_load(list_of_datasets)

    return run()


ghdh_sex_pour = ghdh_sex_dag()

if __name__ == "__main__":
    ghdh_sex_pour.test()
