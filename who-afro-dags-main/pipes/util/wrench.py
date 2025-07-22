"""
This module contains helper functions for all the pipelines.
Think of it as a toolbox with basic wrenches for your plumbing needs.
"""


def get_iso_country_code(country_names):
    """
    This function maps country names to their ISO codes.
    As one dataset can contain multiple countries, we operate on lists.
    Currently CKAN allows only one country per dataset,
    so we return an empty list if there are multiple countries.
    That might change in the future.
    """
    iso_mapping = {
        "Gabon": "GA",
        "Burkina Faso": "BF",
        "Zimbabwe": "ZW",
        "Cameroon": "CM",
        "Mauritania": "MR",
        "Eswatini": "SZ",
        "Nigeria": "NG",
        "Algeria": "DZ",
        "Kenya": "KE",
        "Ghana": "GH",
        "Zambia": "ZM",
        "Cabo Verde": "CV",
        "Lesotho": "LS",
        "South Africa": "ZA",
        "Seychelles": "SC",
        "Central African Republic": "CF",
        "Mauritius": "MU",
        "Senegal": "SN",
        "Malawi": "MW",
        "Democratic Republic of Congo": "CD",
        "Democratic Republic Of Congo": "CD",  # catch alternative capitalisation
        "Mozambique": "MZ",
        "Benin": "BJ",
        "Republic of Congo": "CG",
        "Republic Of Congo": "CG",  # catch alternative capitalisation
        "South Sudan": "SS",
        "Ethiopia": "ET",
        "Burundi": "BI",
        "Guinea": "GN",
        "Mali": "ML",
        "United Republic of Tanzania": "TZ",
        "United Republic Of Tanzania": "TZ",  # catch alternative capitalisation
        "Botswana": "BW",
        "Côte d'Ivoire": "CI",
        "Côte D'Ivoire": "CI",  # catch both capitalised and uncapitalised 'd'
        "Eritrea": "ER",
        "Gambia": "GM",
        "Madagascar": "MG",
        "Namibia": "NA",
        "Niger": "NE",
        "Rwanda": "RW",
        "São Tomé and Príncipe": "ST",
        "São Tomé And Príncipe": "ST",  # catch alternative capitalisation
        "Chad": "TD",
        "Togo": "TG",
        "Uganda": "UG",
        "Angola": "AO",
        "Sierra Leone": "SL",
        "Comoros": "KM",
        "Guinea-Bissau": "GW",
        "Equatorial Guinea": "GQ",
        "Liberia": "LR",
    }
    if (
        country_names == ["AFRO"]
        or country_names == ["AFRO Region"]
        or country_names == ["Africa Region"]
        or country_names == ["African Region"]
    ):
        return ["AFRO"]
    elif len(country_names) > 1:
        # return [val for key, val in iso_mapping.items()] #not possible in CKAN currently
        return []
    else:
        return [iso_mapping.get(name) for name in country_names if name in iso_mapping]
