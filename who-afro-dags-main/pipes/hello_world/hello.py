import os
from pipes.util.load_dotenv import load_local_dotenv
from pipes.util.logger import log

import pendulum
from airflow.decorators import dag, task

load_local_dotenv(file_path=os.path.dirname(__file__))


def print_hello():
    return "Hello world from first Airflow DAG!"


@dag(
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def hello_world_dag():

    @task
    def debug_hello():
        """Print the Airflow context and ds variable from the context."""
        log.debug("Debug log from hello_world")
        return "Hello world from your first Airflow DAG!"

    @task
    def info_hello():
        """Print the Airflow context and ds variable from the context."""
        log.info("Info log from hello_world")
        return "Hello world from your second Airflow DAG!"

    @task
    def warning_hello():
        """Print the Airflow context and ds variable from the context."""
        log.warning("Warning log from hello_world")
        return "Hello world from your third Airflow DAG!"

    @task
    def error_hello():
        """Print the Airflow context and ds variable from the context."""
        log.error("Error log from hello_world")
        return "Hello world from your last Airflow DAG!"

    # pipeline of tasks
    return error_hello() >> warning_hello() >> info_hello() >> debug_hello()


hello_pour = hello_world_dag()

if __name__ == "__main__":
    hello_pour.test()
