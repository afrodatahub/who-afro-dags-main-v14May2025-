# WHO AFRO Apache Airflow DAGs library

## Local development

Dependencies are managed using `pipenv`. To install the dependencies and run the DAGs pipeline locally, run the following commands:

```sh
pipenv install --dev
pipenv run airflow db init
pipenv run python run.py
```

To run `pytest` unit and end-to-end tests, run the following command:

```sh
pipenv run pytest
```

### Using `pyenv` on Ubuntu

We have discovered in that past that if using `pyenv` on Ubuntu systems you may need to install the following:

```sh
sudo apt install libsqlite3-dev libffi-dev
```


## Design principles

These are preliminary design principles; we may choose to change these as we gain more experience with the library and with the data sources.

* One folder per data source
  * There may be more than one pipeline per data source
  * Each folder contains a `README.md` file that describes the data source and the pipeline(s) that are available
  * Any locally held data sources, e.g. XLS files, that we don't want to commit to git should be described in the `README.md` file for that pipeline and put in an `inputs` folder, which git will ignore.
* Environment variables are set using three sets of files: `.env` (provided as an example), `.env.local` and `.env.prod` file at the root of the repository (global) and the root of each data source (for specific environment variables). The scripts should load the variables as available for production, local or example `.env`
  * The files `.env.local` and `.env.prod` are not checked into source control; however, a `.env` file is provided as a template/example for running tests among other things.
  * The global `.env` files are read by the `python-dotenv` library using the `load_dotenv()` function with `prod` and `local` overwriting any example variables inside `run.py`. Dag-specific `.env` files should be loaded the same way
* Each pipeline module should contain 1 DAG that should:
  * Use the `@dag` decorator over the `DAG` class
  * Use the `@task` decorator over the `PythonOperator` class
  * Have useful logging - Airflow logging uses the Python stdlib `logging` module
  * Should initialise the DAG as a "pour", e.g. `new_pour = new_dag()`, within the main body of code
  * Should include a `if __name__ == "__main__":` that runs `dag.test()` if the file is called as a module
* Utility functions should be refactored based on datasource type, e.g. all CKAN-related utility functions should go in `util/ckan.py`
* Pipelines and utils should be tested using the `pytest` module
  * Each pipeline should have a `test_*.py` file that includes end-to-end tests for that pipeline
  * Unit tests should be written for each task and an overall end-to-end test should be written for each DAG
  * Each utility function should have a `test_*.py` file (inside the `util/test` subfolder) that includes unit tests for all utility functions for that datasource
* Each DAG should be added to `run.py` in the root folder - this should run `dag.test()`
