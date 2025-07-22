from urllib.parse import quote_plus

from sqlalchemy import create_engine

from .logger import log


def connect_to_mysql(db_host, db_name, db_user, db_password, ssl=False):
    """Connect to a mysql database"""
    encoded_password = quote_plus(db_password)

    # Database connection URI for MySQL
    database_uri = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}/{db_name}"

    if ssl is False:
        engine = create_engine(database_uri)
    elif ssl == "ssl_no_certs":
        # SSL configuration using an empty dictionary to enforce SSL without certificates, for e.g. for iAHO
        ssl_args = {"ssl": {"ca": None}}
        engine = create_engine(database_uri, connect_args=ssl_args)
    else:
        log.error(f"Unkown ssl configuration: {ssl}")
        raise ValueError("Alternative SSL configurations not implemented.")

    # Create engine with SSL configuration

    # Test connection
    try:
        connection = engine.connect()
        log.info("Connection to the mysql database successful.")
        log.info("MySQL version: %s", connection.execute("SELECT VERSION();").fetchone()[0])
    except Exception as e:
        log.error(f"Connection failed with error: {e}")

    return connection, engine
