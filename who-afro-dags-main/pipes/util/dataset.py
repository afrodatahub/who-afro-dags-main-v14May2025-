"""
A dataset class that uses simple defaults for the metadata required in WHO AFRO CKAN.
"""

import os
from pipes.util import ckan


def generate_dataset(
    title,
    notes="",
    resources={},
    tags=[],
    groups=[],
    country=["AFRO"],
    programme=["other"],
    private=True,
    maintainer="Fjelltopp",
    maintainer_email="info@fjelltopp.org",
    dataset_type="source",
    owner_org=os.getenv("CKAN_ORGANISATION_ID", ""),
):
    clean_title = ckan.clean_name(title.replace(" ", "-"))
    name = clean_title[:100] if len(clean_title) > 100 else clean_title
    return {
        "name": name,
        "title": title,
        "notes": notes,
        "tags": tags,
        "groups": groups,
        "resources": resources,
        "country": country,
        "programme": programme,
        "language": "en_GB",
        "owner_org": owner_org,
        "private": private,
        "maintainer": maintainer,
        "maintainer_email": maintainer_email,
        "type": dataset_type,
    }
