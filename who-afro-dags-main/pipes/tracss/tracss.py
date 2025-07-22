"""
Pipeline to extract country responses to the
Global Database for Tracking Antimicrobial Resistance (AMR)
Country Self- Assessment Survey (TrACSS)
(url:https://amrcountryprogress.org/#/download-responses)
and upload them as a dataset to a CKAN instance
"""

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
def tracss_dag():
    COUNTRY_RESPONSES = [
        (
            "https://amrcountryprogress.org/download/AMR-self-assessment-survey-country-responses-2016-17.xlsx",
            "2016-17",
        ),
        (
            "https://amrcountryprogress.org/download/AMR-self-assessment-survey-country-responses-2017-18.xlsx",
            "2017-18",
        ),
        (
            "https://amrcountryprogress.org/download/AMR-self-assessment-survey-country-responses-2018-19.xls",
            "2018-19",
        ),
        (
            (
                "https://amrcountryprogress.org/download/"
                "AMR%20self%20assessment%20survey%20responses%202019-2020%20(Excel%20format).xls"
            ),
            "2019-20",
        ),
        (
            "https://amrcountryprogress.org/download/Year%20five%20TrACSS%20complete%20data%20for%20publication.xlsx",
            "2020-21",
        ),
        (
            "https://amrcountryprogress.org/download/AMR-self-assessment-survey-responses-TrACSS-2022.xlsx",
            "2022",
        ),
        (
            "https://amrcountryprogress.org/download/AMR-self-assessment-survey-responses-TrACSS-2023.xlsx",
            "2023",
        ),
    ]

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
    ]

    def transform(df):
        if "WHO Region" in df.columns:
            return df[df["WHO Region"] == "AFRO"]  # 2020-21 onwards
        elif "WHO Region " in df.columns:
            return df[df["WHO Region "] == "AFRO"]  # only 2019-20
        elif " WHO Region " in df.columns:  # only 2018-19
            return df[df[" WHO Region "] == "AFRO"]
        elif "WHO Member State" in df.columns:  # only 2017-18
            return df[df["WHO Member State"] == "AFRO"]
        elif "Country" in df.columns:  # only 2016-17
            return df[df["Country"].str.upper().isin(COUNTRY_KEYS)]
        else:
            log.warning("No WHO Region or Country column found in the data; loading all data")
            return df

    def extract_and_transform():
        tags_list = [{"name": "amr"}, {"name": "yearly"}]
        name = "TrACSS African Region Country Responses"
        programme = ["amr"]
        notes = (
            "Global Database for Tracking Antimicrobial Resistance (AMR) Country Self-Assessment Survey "
            "(TrACSS) for WHO African Region member countries 2016-2023."
        )

        resources = []
        for response in COUNTRY_RESPONSES:
            log.info(f"Extracting and transforming {response[1]} data")
            # TODO prior to 2018-19 the publication date is line 2 of these files
            # we should probably add that to the metadata
            if response[1] in ["2016-17", "2017-18", "2020-21"]:
                resources.append(
                    {
                        "name": response[1],
                        "upload": transform(pd.read_excel(response[0], header=2)).to_csv(index=False),
                        "format": "CSV",
                    }
                )
            elif response[1] in ["2018-19"]:
                resources.append(
                    {
                        "name": response[1],
                        "upload": transform(pd.read_excel(response[0], header=1)).to_csv(index=False),
                        "format": "CSV",
                    }
                )
            else:
                resources.append(
                    {
                        "name": response[1],
                        "upload": transform(pd.read_excel(response[0], header=0)).to_csv(index=False),
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
            owner_org="who-afro-dak",
        )

    @task
    def run():
        log.info("Structuring data for CKAN")
        list_of_datasets = [extract_and_transform()]

        ckan.ckan_load(list_of_datasets)

    return run()


tracss_pour = tracss_dag()

if __name__ == "__main__":
    tracss_pour.test()
