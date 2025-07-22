import os
import re
import time
from pipes.util.logger import log

import ckanapi
import requests


def clean_name(dname):
    """
    Cleans the input string to conform to CKAN name restrictions:
    lowercase alphanumeric characters, dashes, and underscores.
    """
    cleaned_name = re.sub(r"[^a-z0-9-_]", "", dname.lower())
    return cleaned_name


def keep_trying(f):
    """
    Retry an api call if it fails the first time.
    CKAN often throws internal errors when bombarded with too many requests.
    """

    def wrapper_function(*args):
        counter = 0
        while True:
            try:
                result = f(*args)
            except ckanapi.errors.ValidationError as e:
                raise e  # Catch all CKANAPIErrors that are not ValidationErrors
            except ckanapi.errors.CKANAPIError as e:
                if counter > 4:
                    raise e  # Raise error after 5 failed attempts
                log.error(f"CKAN API Error: {args[1]['name']}: {e}")
                log.error("Giving CKAN 5s to fix itself before trying again")
                time.sleep(5)
                counter = counter + 1
            except requests.exceptions.ConnectionError as e:
                if counter > 4:
                    raise e  # Raise error after 5 failed attempts
                log.error(f"CKAN Connection Error: {e}")
                log.error("Giving CKAN 5s to fix itself before trying again")
                time.sleep(5)
                counter = counter + 1
            else:
                break
        return result

    return wrapper_function


@keep_trying
def create_or_update_dataset(ckan, dataset):
    try:
        dataset_result = ckan.action.package_create(**dataset)
        log.info(f"Created dataset {dataset_result['name']}")
    except ckanapi.errors.ValidationError as e:
        log.warning(f"Dataset {dataset['name']} might exist. Will try to update.")
        log.info(f"Error: {e}")
        dataset_result = update_dataset(ckan, dataset)
    return dataset_result


@keep_trying
def check_resource_exists(ckan, dataset_id, resource_name):
    dataset = ckan.call_action("package_show", {"id": dataset_id})
    return any(res["name"] == resource_name for res in dataset["resources"])


@keep_trying
def get_existing_resource_id(ckan, resource):
    dataset = ckan.call_action("package_show", {"id": resource["package_id"]})
    return [res for res in dataset["resources"] if res["name"] == resource["name"]][0]["id"] or None


@keep_trying
def update_dataset(ckan, dataset):
    dataset_id = ckan.action.package_show(id=dataset["name"])["id"]
    dataset_result = ckan.action.package_update(id=dataset_id, **dataset)
    log.info(f"Updated dataset {dataset_result['name']}")
    return dataset_result


@keep_trying
def update_resource(ckan, resource, files):
    resource_id = get_existing_resource_id(ckan, resource)
    resource_result = ckan.call_action("resource_update", data_dict={"id": resource_id, **resource}, files=files)
    log.info(f"Updated resource {resource_result['name']}")
    return resource_result


@keep_trying
def create_or_update_resource_from_df(ckan, resource, dataset_id):
    ready_files = {"upload": (f"{resource['name']}.csv", resource["upload"], "text/csv")}
    payload = {
        "package_id": dataset_id,
        **resource,
    }

    if check_resource_exists(ckan, dataset_id, resource["name"]):
        log.info(f"Resource {resource['name']} already exists. Updating.")
        resource = update_resource(ckan, payload, ready_files)
        log.info(f"Updated resource {resource['name']}")
    else:
        resource = ckan.call_action(
            "resource_create",
            data_dict=payload,
            files=ready_files,
        )
        log.info(f"Created resource {payload['name']}")

    return resource


@keep_trying
def delete_dataset(ckan, dataset):
    dataset_id = ckan.action.package_show(id=dataset["name"])["id"]
    ckan.action.package_delete(id=dataset_id)
    log.info(f"Deleted dataset {dataset['name']}")
    return None


def load_dataset(ckan, dataset):
    dataset_payload = dataset
    dataset_resources = dataset.pop("resources", [])
    try:
        dataset_result = create_or_update_dataset(ckan, dataset_payload)
        dataset_id = dataset_result["id"]
        log.info(f"Dataset {dataset_payload['name']} created with id {dataset_id}")

        log.info(f"Creating or updating {len(dataset_resources)} resources in dataset")
        for resource in dataset_resources:
            try:
                create_or_update_resource_from_df(ckan, resource, dataset_id)
            except ckanapi.errors.ValidationError as e:
                log.error(f"Can't create or update resource {resource['name']}: {e.error_dict}")
                break
    except ckanapi.errors.ValidationError as e:
        log.error(f"Can't create or update dataset {dataset['name']}: {e.error_dict}")


def decomission_dataset(ckan, dataset):
    dataset_payload = dataset
    dataset.pop("resources", [])
    try:
        delete_dataset(ckan, dataset_payload)
        log.info(f"Dataset {dataset_payload['name']} deleted")
    except ckanapi.errors.ValidationError as e:
        log.error(f"Can't delete dataset {dataset['name']}: {e.error_dict}")


def ckan_load(list_of_datasets):
    log.info("Uploading data to CKAN")
    ckan_api_token = os.getenv("CKAN_API_TOKEN")
    ckan_url = os.getenv("CKAN_URL")
    log.info(f"Establishing connection with CKAN instance at {ckan_url}")
    ckan_connection = ckanapi.RemoteCKAN(ckan_url, apikey=ckan_api_token)
    log.info("Connection established")

    for dataset in list_of_datasets:
        log.info(f"Loading dataset {dataset['name']}")
        load_dataset(ckan_connection, dataset)

    return None


def ckan_decomission(list_of_datasets):
    log.info("Deleting data from CKAN")
    ckan_api_token = os.getenv("CKAN_API_TOKEN")
    ckan_url = os.getenv("CKAN_URL")
    log.info(f"Establishing connection with CKAN instance at {ckan_url}")
    ckan_connection = ckanapi.RemoteCKAN(ckan_url, apikey=ckan_api_token)
    log.info("Connection established")

    for dataset in list_of_datasets:
        log.info(f"Deleting dataset {dataset['name']}")
        decomission_dataset(ckan_connection, dataset)

    return None
