"""
NOTE: THIS DATA HAS NOW BEEN DECOMMISSIONED
"""

import os
from pipes.util import ckan, dataset, sql, wrench
from pipes.util.load_dotenv import load_local_dotenv
from pipes.util.logger import log

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from sqlalchemy.orm import sessionmaker

load_local_dotenv(file_path=os.path.dirname(__file__))


@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@daily",
    catchup=False,
)
def iaho_dag():
    db_table = "fact_vw_indicator_themes_lookup"

    def format_iaho_country_name(key):
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

    iaho_country_key = [
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
        "AFRO Region",
    ]

    select_distinct_columns = ["indicator", "value", "categoryoption", "country", "measuremethod", "year"]
    select_repeated_columns = ["datasource", "theme_level1", "theme_level2", "theme_level3", "theme_level4"]

    def construct_sql_queries(db_name, db_table, topics_as_themes, topics_as_countries):
        select_distinct_columns_str = ", ".join(select_distinct_columns)
        select_repeated_columns_str = ", ".join(select_repeated_columns)
        select_columns_str = select_distinct_columns_str + ", " + select_repeated_columns_str

        for topic in topics_as_themes:
            # Create SQL query using ROW_NUMBER() to avoid duplicates
            new_query = f"SELECT {select_columns_str} FROM (SELECT {select_columns_str}, ROW_NUMBER() OVER (PARTITION BY {select_distinct_columns_str} ORDER BY {select_repeated_columns[0]}) as rn FROM {db_name}.{db_table} WHERE {topic['col']} IN {tuple(topic['vals'])} ) subquery WHERE rn = 1;"  # noqa: E501
            topic["sql_query"] = new_query

        for country_topic in topics_as_countries:
            country_code = country_topic["vals"][0].replace("'", "\\'")
            new_query = (
                f"SELECT {select_columns_str} from {db_name}.{db_table} WHERE {country_topic['col']}='{country_code}';"
            )
            country_topic["sql_query"] = new_query

        return topics_as_themes, topics_as_countries

    def extract_iaho():
        log.info("Extracting data from iAHO")
        IAHO_USERNAME = os.getenv("IAHO_USERNAME")
        IAHO_PASSWORD = os.getenv("IAHO_PASSWORD")
        IAHO_HOST = os.getenv("IAHO_HOST")
        IAHO_DB = os.getenv("IAHO_DB")
        db_connection, db_engine = sql.connect_to_mysql(
            IAHO_HOST, IAHO_DB, IAHO_USERNAME, IAHO_PASSWORD, ssl="ssl_no_certs"
        )

        topics_as_themes = [
            {
                "name": "HIV",
                "col": "theme_level4",
                "vals": ["HIV/AIDS prevalence", "HIV/AIDS incidence", "HIV/AIDS UHC across communicable diseases"],
                "tags": ["hiv", "yearly", "test"],
            },
            {
                "name": "Malaria",
                "col": "theme_level4",
                "vals": ["Malaria prevalence", "Malaria incidence", "Malaria UHC across communicable diseases"],
                "tags": ["malaria", "yearly", "test"],
            },
            {
                "name": "TB",
                "col": "theme_level4",
                "vals": [
                    "Tuberculosis prevalence",
                    "Tuberculosis incidence",
                    "Tuberculosis UHC across communicable diseases",
                ],
                "tags": ["tuberculosis", "yearly", "test", "tb"],
            },
        ]

        # Create topics_as_countries
        topics_as_countries = []
        for country_key in iaho_country_key:
            formatted_country_name = format_iaho_country_name(country_key)
            topic = {"name": formatted_country_name, "col": "country", "vals": [country_key]}

            for theme in topics_as_themes:
                topic["tags"] = ["yearly"] + [t["name"] for t in topics_as_themes]
            topics_as_countries.append(topic)

        log.info(f"Preparing SQL queries for {len(topics_as_themes)} themes and {len(topics_as_countries)} countries.")
        topics_as_themes, topics_as_countries = construct_sql_queries(
            IAHO_DB, db_table, topics_as_themes, topics_as_countries
        )

        log.info("Executing SQL queries for theme-based topics")
        Session = sessionmaker(bind=db_engine)
        session = Session()
        for topic in topics_as_themes:
            log.debug(f"Executing query for {topic['name']}")
            log.debug(topic["sql_query"])
            result = session.execute(topic["sql_query"])
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            topic["df"] = result_df
        log.info("Executing SQL queries for country-based data")
        for topic in topics_as_countries:
            log.debug(f"Executing query for {topic['name']}")
            log.debug(topic["sql_query"])
            result = session.execute(topic["sql_query"])
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            topic["df"] = result_df
        session.close()
        log.info("Data extraction via SQL complete.")
        return topics_as_themes, topics_as_countries

    def prepare_iaho_data(topics_as_themes, topics_as_countries):
        log.info("Preparing data for CKAN upload as a list of datasets")
        list_of_datasets = []
        for topic in topics_as_themes:
            list_of_datasets.append(prepare_ckan_dataset(topic, "theme"))

        for topic in topics_as_countries:
            list_of_datasets.append(prepare_ckan_dataset(topic, "country"))

        return list_of_datasets

    def prepare_ckan_dataset(topic, topic_type):
        if topic_type == "country":
            tags_list = [{"name": str(tag)} for tag in topic["tags"]]
            iso_country_codes = wrench.get_iso_country_code([topic["name"]])
            name = f"iAHO data: {topic['name']}"
            resources = [
                {
                    "name": name,
                    "upload": topic["df"].to_csv(index=False),
                    "format": "CSV",
                }
            ]
            programme = ["other"]
        elif topic_type == "theme":
            tags_list = [{"name": str(tag)} for tag in topic["tags"]]
            iso_country_codes = ["AFRO"]
            name = f"iAHO data: {topic['name']}"
            resources = []
            for ck in iaho_country_key:
                resource_name = f"{topic['name']} - {format_iaho_country_name(ck)}"
                resources.append(
                    {
                        "name": resource_name,
                        "upload": topic["df"][topic["df"]["country"] == ck].to_csv(index=False),
                        "format": "CSV",
                    }
                )
            programme = [topic["name"].lower()]
        else:
            raise ValueError(f"Unknown topic type: {topic_type}")

        return dataset.generate_dataset(
            name,
            tags=tags_list,
            country=iso_country_codes,
            programme=programme,
            private=False,
            notes=(
                f"Indicator data for {name} from the Integrated Analytical Health Observatory (iAHO). "
                f"Using an sql query {topic['sql_query']}"
            ),
            resources=resources,
            owner_org="who-afro-dak",
        )

    @task
    def run():
        topics_as_themes, topics_as_countries = extract_iaho()

        list_of_datasets = prepare_iaho_data(topics_as_themes, topics_as_countries)

        ckan.ckan_decomission(list_of_datasets)

    return run()


iaho_pour = iaho_dag()

if __name__ == "__main__":
    iaho_pour.test()
