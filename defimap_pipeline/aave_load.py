# TO CHANGE THIS TEMPLATE TO TO OTHER STRATEGY
# edit load_strategy_apr and the variable protocol_name

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import pandas as pd
from google.cloud import bigquery
import sqlalchemy
import urllib
from dateutil import parser
from pendulum import DateTime

# config
########################################################################
protocol_name = "aave"
########################################################################
data_mart = Variable.get("data_mart", default_var={}, deserialize_json=True)

STRATEGIES = Variable.get(f"{protocol_name}-strategies", deserialize_json=True)

RAW_DATASET_ID = "defimap.raw_data"
TVL_DATASET_ID = "defimap.tvl"
GOTK_DATASET_ID = "defimap.growth_of_10k"

db_conn_string, db_username, db_password, db_database = (
    data_mart["conn_string"],
    data_mart["username"],
    data_mart["password"],
    data_mart["database"],
)


# helper function
def db_url():
    return f"postgresql://{db_username}:{urllib.parse.quote(db_password)}@{db_conn_string}/{db_database}"


def get_strategy_id(db_conn, strategy_name: str):
    strategy_id_result = db_conn.execute(
        f"""--sql
            SELECT id FROM public.strategy
            WHERE "slug" = '{strategy_name}'
            """
    )

    strategy_id = str(strategy_id_result.first()[0])

    return strategy_id


def load_strategy_gotk(strategy_name: str):
    url = db_url()
    engine = sqlalchemy.create_engine(url)

    with engine.connect() as db_conn:
        client = bigquery.Client()
        # df gotk
        df_gotk = client.query(
            f"""
            SELECT date, start_day_investment, end_day_investment
            FROM `{GOTK_DATASET_ID}.{strategy_name}`

        """
        ).to_dataframe()

        strategy_id = get_strategy_id(db_conn, strategy_name)
        df_gotk["strategy_id"] = strategy_id

        # delete old data
        db_conn.execute(
            f"""--sql
            DELETE FROM public.strategy_growth
            WHERE "strategy_id" = '{strategy_id}'
            """
        )

        # insert
        df_gotk.to_sql(
            "strategy_growth", db_conn, if_exists="append", index=False, schema="public"
        )
        db_conn.connection.close()


def load_strategy_tvl(strategy_name: str):
    url = db_url()
    engine = sqlalchemy.create_engine(url)

    with engine.connect() as db_conn:
        client = bigquery.Client()
        df_tvl = client.query(
            f"""
                SELECT date, tvl, change_tvl as change_tvl_daily, percent_change FROM `{TVL_DATASET_ID}.{strategy_name}`
                ORDER BY date DESC
            """
        ).to_dataframe()

        df_tvl.dropna(inplace=True)
        strategy_id = get_strategy_id(db_conn, strategy_name)
        df_tvl["strategy_id"] = strategy_id
        df_tvl["change_tvl_monthly"] = 0
        df_tvl["change_tvl_yearly"] = 0

        latest_tvl = df_tvl["tvl"][0]

        # delete old data
        db_conn.execute(
            f"""--sql
            DELETE FROM public.strategy_tvl
            WHERE "strategy_id" = '{strategy_id}'
            """
        )

        # insert
        df_tvl.to_sql(
            "strategy_tvl", db_conn, if_exists="append", index=False, schema="public"
        )

        db_conn.connection.cursor().execute(
            f"""--sql
                UPDATE strategy
                SET "tvl" = {latest_tvl}
                WHERE "id" = '{strategy_id}';
            """
        )
        db_conn.connection.commit()
        db_conn.connection.close()


def load_strategy_apr(strategy_name: str):
    url = db_url()
    engine = sqlalchemy.create_engine(url)

    with engine.connect() as db_conn:
        client = bigquery.Client()
        df_apr = client.query(
            f"""
                SELECT date as timestamp, total_apy as value FROM `{RAW_DATASET_ID}.{strategy_name}` ORDER BY date DESC
            """
        ).to_dataframe()
        strategy_id = get_strategy_id(db_conn, strategy_name)
        df_apr["strategy_id"] = strategy_id
        latest_apr = df_apr["value"][0]

        # delete old data
        db_conn.execute(
            f"""--sql
            DELETE FROM public.strategy_apr
            WHERE "strategy_id" = '{strategy_id}'
            """
        )

        # insert
        df_apr.to_sql(
            "strategy_apr", db_conn, if_exists="append", index=False, schema="public"
        )

        db_conn.connection.cursor().execute(
            f"""--sql
            UPDATE public.strategy
            SET "apr" = {latest_apr}
            WHERE "id" = '{strategy_id}';
            """
        )
        db_conn.connection.commit()
        db_conn.connection.close()


def create_load_dag(
    dag_id: str,
    description: str,
    schedule_interval: str,
    start_date: DateTime,
    tags: list,
):
    dag = DAG(
        dag_id,
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        tags=tags,
        default_args={"retries": 5},
        max_active_runs=1,
    )

    start_pipeline_task = EmptyOperator(
        task_id="start_pipeline",
        dag=dag,
        # depends_on_past=True,
        # wait_for_downstream=True,
    )

    end_pipeline_task = EmptyOperator(
        task_id="end_pipeline",
        dag=dag,
    )

    for key, _ in STRATEGIES.items():
        strategy_name = f"{protocol_name}_{key}"
        load_gotk_task = PythonOperator(
            dag=dag,
            task_id=f"load_{strategy_name}_gotk",
            provide_context=True,
            python_callable=load_strategy_gotk,
            op_kwargs={"strategy_name": strategy_name},
        )
        load_tvl_task = PythonOperator(
            dag=dag,
            task_id=f"load_{strategy_name}_tvl",
            provide_context=True,
            python_callable=load_strategy_tvl,
            op_kwargs={"strategy_name": strategy_name},
        )
        load_apr_task = PythonOperator(
            dag=dag,
            task_id=f"load_{strategy_name}_apr",
            provide_context=True,
            python_callable=load_strategy_apr,
            op_kwargs={"strategy_name": strategy_name},
        )

        (
            start_pipeline_task
            >> load_gotk_task
            >> load_tvl_task
            >> load_apr_task
            >> end_pipeline_task
        )

    return dag


globals()[f"{protocol_name}_load_to_datamart"] = create_load_dag(
    dag_id=f"{protocol_name}_load_to_datamart",
    description=f"Load {protocol_name} strategy to datamart",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 3, 20, 0, 0, 0, tz="UTC"),
    tags=[f"{protocol_name}", "load_to_datamart"],
)
