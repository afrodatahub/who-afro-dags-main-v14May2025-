"""
This module loads environment variables from a .env file in the current directory.
If there is a .env.local file, it will load that as well, overriding any variables in the .env file.
If there is a .env.prod file, it will load that as well, overriding any variables in the .env.local file.
It then resets the working directory to the original working directory.
"""

import os

from dotenv import load_dotenv

from .logger import log


def load_local_dotenv(file_path=None):
    cwd = os.getcwd()
    if file_path:
        os.chdir(file_path)
    log.debug(f"Getting .env files from directory: {os.getcwd()}")
    load_dotenv(".env")
    if os.path.isfile(".env.local"):
        log.info("Loading local environment variables")
        load_dotenv(".env.local", override=True)
    if os.path.isfile(".env.prod"):
        log.info("Loading production environment variables")
        load_dotenv(".env.prod", override=True)
    os.chdir(cwd)
    log.debug("Current environment variables:")
    log.debug(os.environ)
