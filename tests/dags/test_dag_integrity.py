"""DAG integrity test (run inside Astro with: astro dev pytest)."""
from airflow.models import DagBag


def test_dag_loads_with_three_tasks():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors

    # read the parsed DAG directly: in Airflow 3 get_dag() hits the metadata DB,
    # which is empty inside the `astro dev pytest` container
    dag = dagbag.dags.get("currency_exchange_etl")
    assert dag is not None
    assert set(dag.task_ids) == {
        "extract_exchange_rates",
        "transform_exchange_rates",
        "load_exchange_rates",
    }
    assert dag.get_task("extract_exchange_rates").downstream_task_ids == {"transform_exchange_rates"}
    assert dag.get_task("transform_exchange_rates").downstream_task_ids == {"load_exchange_rates"}
