"""
Shared logger for all modules in the project -
  essentially so we can make changes in one place and
  have it apply everywhere whether we run the code:
  whether in a notebook or in a script or as a DAG in Apache Airflow
"""

import logging

log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(logging.INFO)
