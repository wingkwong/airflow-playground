from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from random import randint


def _random_func():
    return randint(1, 100)


def _choose_best(ti):
    candidates = ti.xcom_pull(task_ids=["task_A", "task_B", "task_C"])
    best = max(candidates)
    if best >= 90:
        return "other_task_A"
    elif best <= 10:
        return "other_task_B"
    return "dummy_task"


with DAG(
    "hello-dag",
    start_date=datetime(2021, 6, 21),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    some_python_tasks = [
        PythonOperator(
            task_id=f"task_{idx}", python_callable=_random_func, op_kwargs={"idx": idx}
        )
        for idx in ["A", "B", "C"]
    ]

    choose_the_best = BranchPythonOperator(
        task_id="choose_the_best", python_callable=_choose_best
    )

    other_task_A = BashOperator(
        task_id="other_task_A", bash_command="echo 'Other Task A'"
    )

    other_task_B = BashOperator(
        task_id="other_task_B", bash_command="echo 'Other Task B'"
    )

    dummy_task_A = DummyOperator(task_id="dummy_task_A")

    dummy_task_B = DummyOperator(task_id="dummy_task_B")

    (some_python_tasks >> choose_the_best >> [other_task_A, other_task_B, dummy_task_A])

    other_task_B >> dummy_task_B
