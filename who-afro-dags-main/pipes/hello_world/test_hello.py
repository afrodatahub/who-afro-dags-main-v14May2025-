import logging
from pipes.hello_world.hello import hello_world_dag

import pendulum
import pytest
from airflow.models import DagBag

DAG_ID = "hello_world_dag"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=".", include_examples=False)


def test_dag_loaded(dagbag):
    logging.info(f"Testing DAG {DAG_ID}")
    logging.debug(f"DAGs available: {dagbag.dags.keys()}")
    assert DAG_ID in dagbag.dags


def test_task_count(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert len(dag.tasks) == 4


def test_task_dependencies(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    tasks = dag.tasks

    assert tasks[0] in tasks[1].upstream_list
    assert tasks[1] in tasks[2].upstream_list
    assert tasks[2] in tasks[3].upstream_list


def test_hello_world_dag():
    dag = hello_world_dag()
    assert dag.start_date == pendulum.datetime(2024, 1, 1, tz="UTC")
    assert dag.schedule_interval == "@daily"
    assert dag.catchup == False  # noqa: E712

    tasks = dag.tasks
    assert len(tasks) == 4

    assert tasks[0].__class__.__name__ == "_PythonDecoratedOperator"
    assert tasks[0].python_callable.__name__ == "error_hello"

    assert tasks[1].__class__.__name__ == "_PythonDecoratedOperator"
    assert tasks[1].python_callable.__name__ == "warning_hello"

    assert tasks[2].__class__.__name__ == "_PythonDecoratedOperator"
    assert tasks[2].python_callable.__name__ == "info_hello"

    assert tasks[3].__class__.__name__ == "_PythonDecoratedOperator"
    assert tasks[3].python_callable.__name__ == "debug_hello"
