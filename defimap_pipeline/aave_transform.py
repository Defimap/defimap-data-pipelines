# TO CHANGE THIS TEMPLATE TO TO OTHER STRATEGY
# edit transform_tvl, get_gotk_query, get_total_return_query,
# get_trailing_return_query, get_benchmark_query condition(stablecoin/not)
# SENSORS for benchmark and the variable protocol_name

import math
import statistics
import numpy as np
import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery

# config
########################################################################
protocol_name = "aave"
########################################################################


INITIAL_PRINCIPAL = 10000
INITIAL_DATE = "2022-01-01"

TOKEN_PRICE_DATASET = "defimap.token_price"
RAW_DATASET_ID = "defimap.raw_data"
GOTK_DATASET = "defimap.growth_of_10k"
TVL_DATASET_ID = "defimap.tvl"
PRE_TOTAL_RETURN_DATASET_ID = "defimap.pre_total_return"
PRE_TRAILING_RETURN_DATASET_ID = "defimap.pre_trailing_return"
PRE_RISK_DATASET_ID = "defimap.pre_risk"

STRATEGIES = Variable.get(f"{protocol_name}-strategies", deserialize_json=True)
STABLECOINS = Variable.get("stablecoins", deserialize_json=True)


# query getter
def get_benchmark_query(start_date: str, end_date: str, strategy_id: str):
    if strategy_id.split("_")[1] in STABLECOINS:
        query = f"""--sql
            SELECT 
                `date`, 
                'aave_usdc' as `name`,
                IFNULL(LAG(`end_day_investment`) OVER (ORDER BY `date` ASC), {INITIAL_PRINCIPAL}) as `start_day_investment`,
                `end_day_investment`,
                `end_day_investment` / IFNULL(LAG(`end_day_investment`) 
            OVER (ORDER BY `date` ASC), {INITIAL_PRINCIPAL}) -1 as `percent_change`
            FROM (
                SELECT 
                    `date`, 
                    {INITIAL_PRINCIPAL} / FIRST_VALUE(`asset_price`) OVER (ORDER BY `date` ASC)
                        * `liquidity_index` / FIRST_VALUE(`liquidity_index`) OVER (ORDER BY `date` ASC)
                        * `asset_price`  
                    as `end_day_investment`,
            FROM `{RAW_DATASET_ID}.aave_usdc` 
            WHERE `date` BETWEEN '{start_date}' AND '{end_date}'
            )
            ORDER BY `date` ASC
        """
    else:
        query = f"""--sql
            SELECT
                `date`,
                `name`,
                `start_day_investment`,
                `end_day_investment`,
                SAFE_DIVIDE(`end_day_investment` - `start_day_investment`, `start_day_investment`) AS `percent_change`
            FROM (
                SELECT
                    `date`,
                    `name`,
                    IFNULL(LAG(`end_day_investment`) OVER (ORDER BY `date`), {INITIAL_PRINCIPAL}) AS `start_day_investment`,
                    `end_day_investment`
                FROM (
                    SELECT
                        `date`,
                        `name`,
                        `price_usd` as `asset_price`,
                        ({INITIAL_PRINCIPAL} / (FIRST_VALUE(`price_usd`) OVER (ORDER BY `date` ASC))) * `price_usd` AS `end_day_investment`
                    FROM  
                        `{TOKEN_PRICE_DATASET}.wbtc`
                    WHERE `date` BETWEEN '{start_date}' AND '{end_date}'
                )
            )
            ORDER BY `date` ASC
        """
    return query


def get_gotk_query(start_date: str, end_date: str, strategy_id: str):
    query = f"""--sql
            SELECT 
                `date`, 
                '{strategy_id}' as `name`,
                IFNULL(LAG(`end_day_investment`) OVER (ORDER BY `date` ASC), {INITIAL_PRINCIPAL}) as `start_day_investment`,
                `end_day_investment`,
                `end_day_investment` / IFNULL(LAG(`end_day_investment`) OVER (ORDER BY `date` ASC), {INITIAL_PRINCIPAL}) -1 as `percent_change`
            FROM (
                SELECT 
                    `date`, 
                    `end_day_aave_reward` * `aave_price` + `end_day_capital` as `end_day_investment`
                FROM (
                    SELECT 
                        `date`,
                        SUM((`end_day_capital` * `reward_rate`) / `aave_price`) 
                            OVER (ORDER BY `date` ROWS UNBOUNDED PRECEDING
                        ) as `end_day_aave_reward`,
                        `end_day_capital`,
                        `aave_price`
                    FROM (
                        SELECT 
                            `date`, 
                            `aave_apy`/365/100 as `reward_rate`,
                            {INITIAL_PRINCIPAL} / FIRST_VALUE(`asset_price`) OVER (ORDER BY `date` ASC)
                            * `liquidity_index` / FIRST_VALUE(`liquidity_index`) OVER (ORDER BY `date` ASC)
                            * `asset_price`  as `end_day_capital`,
                            `aave_price`
                        FROM 
                            `{RAW_DATASET_ID}.{strategy_id}` 
                        WHERE `date` BETWEEN '{start_date}' AND '{end_date}'
                    )
                )
            )
            ORDER BY `date` ASC
    """

    return query


def get_total_return_query(strategy_id):
    query = f"""--sql
        SELECT 
            `date`, 
            '{strategy_id}' as `name`,
            `end_day_investment` / IFNULL(LAG(`end_day_investment`) OVER (PARTITION BY FORMAT_DATE('%Y-%m', `date`) ORDER BY `date` ASC), {INITIAL_PRINCIPAL}) -1 as `percent_change`
        FROM (
            SELECT 
                `date`, 
                `end_day_aave_reward` * `aave_price` + `end_day_capital` as `end_day_investment`
            FROM (
                SELECT 
                `date`,
                SUM((`end_day_capital` * `reward_rate`) / `aave_price`) 
                    OVER (PARTITION BY FORMAT_DATE('%Y-%m', `date`) ORDER BY `date` ROWS UNBOUNDED PRECEDING
                ) as `end_day_aave_reward`,
                `end_day_capital`,
                `aave_price`
                FROM (
                    SELECT 
                        `date`, 
                        `aave_apy`/365/100 as `reward_rate`,
                        {INITIAL_PRINCIPAL} / FIRST_VALUE(`asset_price`) OVER (PARTITION BY FORMAT_DATE('%Y-%m', `date`) ORDER BY `date` ASC)
                        * `liquidity_index` / FIRST_VALUE(`liquidity_index`) OVER (PARTITION BY FORMAT_DATE('%Y-%m', `date`) ORDER BY `date` ASC)
                        * `asset_price`  as `end_day_capital`,
                        `aave_price`
                    FROM 
                        `{RAW_DATASET_ID}.{strategy_id}` 
                )
            )
        )
        ORDER BY `date` ASC
        """

    return query


def get_trailing_return_query(ds, strategy_id):
    periods = [("1d", 1), ("1m", 30), ("3m", 90), ("6m", 180), ("1y", 365)]

    query = f"""--sql
        SELECT 
            `date`,
            `period`,
            '{strategy_id}' as `name`,
            `percent_change`
        FROM (
    """
    for period, day in periods:
        start_date = (
            pendulum.from_format(ds, "YYYY-MM-DD")
            .subtract(days=day)
            .strftime("%Y-%m-%d")
        )
        query += f"""--sql
        (
        SELECT 
            '{period}' as `period`,
            `date`,
            IF(FIRST_VALUE(`date`) OVER (ORDER BY `date` ASC) = '{start_date}', 
            `percent_change`,
            NULL) AS `percent_change`
        FROM (
            {get_gotk_query(start_date, ds, strategy_id)}
        )
        ) UNION ALL
        """

    start_of_year = pendulum.date(
        pendulum.from_format(ds, "YYYY-MM-DD").year, month=1, day=1
    ).strftime("%Y-%m-%d")
    query += f"""--sql
        (
        SELECT 
            'ytd' as `period`,
            `date`, 
            `percent_change`
        FROM (
            {get_gotk_query(start_of_year, ds, strategy_id)}
        )
        )
    """
    query += ") ORDER BY `period`, `date`"
    return query


# transform
def transform_gotk(ds: str, table_id: str, start_date: str):
    client = bigquery.Client()

    query = get_gotk_query(start_date, ds, table_id)

    df = client.query(query).to_dataframe()
    gotk_table_id = f"{GOTK_DATASET}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("start_day_investment", "FLOAT"),
            bigquery.SchemaField("end_day_investment", "FLOAT"),
            bigquery.SchemaField("percent_change", "FLOAT"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    result = client.load_table_from_dataframe(
        df, gotk_table_id, job_config=job_config
    ).result()

    if str(result.state).lower() == "done":
        print("Loaded rows {} to table {}".format(len(df), gotk_table_id))


def transform_tvl(ds: str, table_id: str):
    client = bigquery.Client()

    raw_table_id = f"{RAW_DATASET_ID}.{table_id}"
    query = f"""--sql
        SELECT
            `date`,
            `name`,
            `tvl`,
            `tvl` - IFNULL(LAG(`tvl`) OVER (ORDER BY `date` ASC), `tvl`) AS `change_tvl`,
            IFNULL((`tvl` - IFNULL(LAG(`tvl`) OVER (ORDER BY `date` ASC), `tvl`)) / NULLIF(`tvl`, 0), 0) * 100 as `percent_change`
        FROM
        (
        SELECT            
            `date`,
            `name`,
            `atoken_supply` * `asset_price` as `tvl`
        FROM
            {raw_table_id}
        WHERE
            `date` <= '{ds}'
        ORDER BY
            `date` ASC
        )
    """

    df = client.query(query).to_dataframe()

    index_table_id = f"{TVL_DATASET_ID}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("tvl", "FLOAT"),
            bigquery.SchemaField("change_tvl", "FLOAT"),
            bigquery.SchemaField("percent_change", "FLOAT"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    result = client.load_table_from_dataframe(
        df, index_table_id, job_config=job_config
    ).result()

    if str(result.state).lower() == "done":
        print("Loaded rows {} to table {}".format(len(df), index_table_id))


def transform_pre_total_return(ds: str, table_id: str):
    client = bigquery.Client()

    query = get_total_return_query(table_id)

    df_pre_total_return = client.query(query).to_dataframe()
    pre_total_return_table_id = f"{PRE_TOTAL_RETURN_DATASET_ID}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("percent_change", "FLOAT"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    result = client.load_table_from_dataframe(
        df_pre_total_return, pre_total_return_table_id, job_config=job_config
    ).result()

    if str(result.state).lower() == "done":
        print(
            "Loaded rows {} to table {}".format(
                len(df_pre_total_return), pre_total_return_table_id
            )
        )


def transform_pre_trailing_return(ds: str, table_id: str, **kwargs):
    client = bigquery.Client()

    query = get_trailing_return_query(ds, table_id)

    df_pre_trailing_return = client.query(query).to_dataframe()
    pre_trailing_return_table_id = f"{PRE_TRAILING_RETURN_DATASET_ID}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("period", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("percent_change", "FLOAT"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    result = client.load_table_from_dataframe(
        df_pre_trailing_return, pre_trailing_return_table_id, job_config=job_config
    ).result()

    if str(result.state).lower() == "done":
        print(
            "Loaded rows {} to table {}".format(
                len(df_pre_trailing_return), pre_trailing_return_table_id
            )
        )


def transform_pre_risk(ds: str, table_id: str, **kwargs):
    def find_sd(df_gotk: pd.DataFrame, day_period: int):
        data = df_gotk["percent_change"].tail(day_period)
        sd = statistics.stdev(data)
        annual_sd = sd * math.sqrt(365)
        return annual_sd

    def find_average_t_return(df_gotk: pd.DataFrame, day_period: int):
        day_return = df_gotk["percent_change"].tail(day_period).mean()
        annual_return = ((day_return + 1) ** day_period) - 1
        return annual_return

    def find_sharpe_ratio(annual_return: float, sd: float):
        return (annual_return) / sd

    def get_alpha_beta_r2(
        df_category: pd.DataFrame, df_gotk: pd.DataFrame, day_period: int
    ):
        if len(df_category.index) < day_period or len(df_gotk.index) < day_period:
            day_period = min(len(df_category.index), len(df_gotk.index))
        x = df_category["percent_change"].tail(day_period).reset_index(drop=True)
        y = df_gotk["percent_change"].tail(day_period).reset_index(drop=True)
        theta = np.polyfit(x, y, 1)
        unexp_v = []
        total_v = []
        avg_y = y.mean()
        for i in range(len(x)):
            predicted_y = theta[1] + theta[0] * x[i]
            actual_y = y[i]
            unexp_v.append((predicted_y - actual_y) ** 2)
            total_v.append((actual_y - avg_y) ** 2)
        unexp_v = sum(unexp_v)
        total_v = sum(total_v)
        r_squared = 1 - (unexp_v / total_v)
        return (theta[1], theta[0], r_squared)

    def find_max_drawdown(df_gotk: pd.DataFrame, day_period: int):
        df_gotk = df_gotk.tail(day_period).reset_index(drop=True)
        df_gotk["returns"] = (
            df_gotk["end_day_investment"] - df_gotk["start_day_investment"]
        )
        df_gotk["cumulative_returns"] = df_gotk["returns"].cumsum()
        df_gotk["running_max"] = df_gotk["cumulative_returns"].cummax()
        df_gotk["drawdown"] = df_gotk["cumulative_returns"] - df_gotk["running_max"]

        trough_date = df_gotk["date"][df_gotk["drawdown"].idxmin()]
        df_gotk_trough_date = df_gotk[df_gotk["date"] <= trough_date]
        peak_date = df_gotk_trough_date["date"][
            df_gotk_trough_date["cumulative_returns"].idxmax()
        ]
        max_drawdown = df_gotk["drawdown"].min()
        drawdown_period = (trough_date - peak_date).days
        find_index = df_gotk.index[df_gotk["date"] == peak_date].tolist()
        index = int(find_index[0])
        mm = max_drawdown / df_gotk["end_day_investment"][index]
        return max_drawdown, mm, peak_date, trough_date, drawdown_period

    # get last year date as "YYYY-MM-DD"
    last_year = (
        pendulum.from_format(ds, "YYYY-MM-DD").subtract(days=365).strftime("%Y-%m-%d")
    )
    client = bigquery.Client()

    # get 1y return
    pre_trailing_return_table_id = f"{PRE_TRAILING_RETURN_DATASET_ID}.{table_id}"
    query = f"""--sql
        SELECT *
        FROM (
            SELECT
                *,
                (EXP(SUM(LOG(`percent_change` + 1)) OVER (PARTITION BY `period` ORDER BY `date` ASC)) - 1) as `value`,
            FROM
                `{pre_trailing_return_table_id}`
        )
        WHERE
            `date` = '{ds}'
        AND
            `period` = '1y'
    """
    return_1y = client.query(query).to_dataframe()["value"][0]
    if float(return_1y) == 0.0:
        query = f"""--sql
            SELECT *
            FROM (
                SELECT
                    *,
                    (EXP(SUM(LOG(`percent_change` + 1)) OVER (PARTITION BY `period` ORDER BY `date` ASC)) - 1) as `value`,
                FROM
                    `{pre_trailing_return_table_id}`
            )
            WHERE
                `date` = '{ds}'
            AND
                `period` = 'ytd'
        """
        return_1y = client.query(query).to_dataframe()["value"][0]

    # get strategy's gotk
    query = get_gotk_query(last_year, ds, table_id)
    df_gotk = client.query(query).to_dataframe()

    # get benchmark df
    query = get_benchmark_query(last_year, ds, table_id)
    df_benchmark = client.query(query).to_dataframe()

    # get metrics
    sd_1y = find_sd(df_gotk, 365)
    annual_return = find_average_t_return(df_gotk, 365)
    sharpe_1y = find_sharpe_ratio(annual_return, sd_1y)
    (alpha, beta, r_square) = get_alpha_beta_r2(df_benchmark, df_gotk, 365)
    (
        _,
        max_drawdown,
        drawdown_peak_date,
        drawdown_valley_date,
        drawdown_period,
    ) = find_max_drawdown(df_gotk, 365)

    data = {
        "date": pd.to_datetime(ds),
        "name": table_id,
        "sd": sd_1y,
        "return_1y": return_1y,
        "sharpe": sharpe_1y,
        "alpha": alpha,
        "beta": beta,
        "r_square": r_square,
        "max_drawdown": max_drawdown,
        "peak_date": drawdown_peak_date,
        "valley_date": drawdown_valley_date,
        "duration": drawdown_period,
    }

    df_risk = pd.DataFrame([data])

    risk_table_id = f"{PRE_RISK_DATASET_ID}.{table_id}"
    # delete existing table
    if not kwargs["prev_data_interval_end_success"]:
        print("Running for the first time, clearing existing table")
        client.delete_table(risk_table_id, not_found_ok=True)
        print(f"Deleted existing table: {risk_table_id}")
    else:
        query = f"""--sql
            DELETE
            FROM
                `{risk_table_id}`
            WHERE
                `date` = '{ds}'
        """
        client.query(query)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("sd", "FLOAT"),
            bigquery.SchemaField("return_1y", "FLOAT"),
            bigquery.SchemaField("sharpe", "FLOAT"),
            bigquery.SchemaField("alpha", "FLOAT"),
            bigquery.SchemaField("beta", "FLOAT"),
            bigquery.SchemaField("r_square", "FLOAT"),
            bigquery.SchemaField("max_drawdown", "FLOAT"),
            bigquery.SchemaField("peak_date", "DATE"),
            bigquery.SchemaField("valley_date", "DATE"),
            bigquery.SchemaField("duration", "INTEGER"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    result = client.load_table_from_dataframe(
        df_risk, risk_table_id, job_config=job_config
    ).result()

    if str(result.state).lower() == "done":
        print("Loaded rows {} to table {}".format(len(df_risk), risk_table_id))


with DAG(
    dag_id=f"{protocol_name}_transform",
    description=f"Transform {protocol_name} Supply",
    tags=[f"{protocol_name}", "transform"],
    start_date=pendulum.datetime(2023, 3, 20, 0, 0, 0, tz="UTC"),
    schedule_interval=None,
    max_active_runs=1,
    catchup=True,
) as dag:
    start_pipeline_task = EmptyOperator(
        task_id="start_pipeline",
        dag=dag,
        # depends_on_past=True,
        # wait_for_downstream=True,
    )
    finish_gotk_task = EmptyOperator(
        task_id="finish_gotk",
        dag=dag,
    )
    finish_pre_total_return_task = EmptyOperator(
        task_id="finish_pre_total_return",
        dag=dag,
    )
    finish_pre_trailing_return_task = EmptyOperator(
        task_id="finish_pre_trailing_return",
        dag=dag,
    )
    finish_pre_risk_task = EmptyOperator(
        task_id="finish_pre_risk",
        dag=dag,
    )
    # SENSORS
    aave_usdc_sensor = ExternalTaskSensor(
        task_id=f"aave_usdc_sensor",
        dag=dag,
        external_dag_id="aave_extract",
        external_task_ids=["load_to_bq_usdc"],
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=2 * 60,  # 2 minutes
        timeout=30 * 60,  # 30 mins
        # execution_delta=timedelta(minutes=30),
    )
    wbtc_sensor = ExternalTaskSensor(
        task_id=f"wbtc_sensor",
        dag=dag,
        external_dag_id="common_extract_price",
        external_task_ids=["load_to_bq_wbtc"],
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=2 * 60,  # 2 minutes
        timeout=30 * 60,  # 30 mins
        # execution_delta=timedelta(minutes=30),
    )
    trigger_load_task = TriggerDagRunOperator(
        dag=dag,
        task_id="trigger_load_to_datamart",
        trigger_dag_id=f"{protocol_name}_load_to_datamart",
        reset_dag_run=True,
        execution_date="{{ ds }}",
        allowed_states=["success", "skipped"],
    )
    end_pipeline_task = EmptyOperator(
        task_id="end_pipeline",
        dag=dag,
    )
    for token, _ in STRATEGIES.items():
        table_id = f"{protocol_name}_{token}"

        transform_gotk_task = PythonOperator(
            task_id=f"transform_gotk_{table_id}",
            python_callable=transform_gotk,
            provide_context=True,
            op_kwargs={"table_id": table_id, "start_date": INITIAL_DATE},
        )

        transform_tvl_task = PythonOperator(
            task_id=f"transform_tvl_{table_id}",
            python_callable=transform_tvl,
            provide_context=True,
            op_kwargs={"table_id": table_id},
        )

        transform_pre_total_return_task = PythonOperator(
            task_id=f"transform_pre_total_return_{table_id}",
            python_callable=transform_pre_total_return,
            provide_context=True,
            op_kwargs={"table_id": table_id},
        )

        transform_pre_trailing_return_task = PythonOperator(
            task_id=f"transform_pre_trailing_return_{table_id}",
            python_callable=transform_pre_trailing_return,
            provide_context=True,
            op_kwargs={"table_id": table_id},
        )
        transform_pre_risk_task = PythonOperator(
            task_id=f"transform_pre_risk_{table_id}",
            python_callable=transform_pre_risk,
            provide_context=True,
            op_kwargs={"table_id": table_id},
        )

        (
            transform_pre_total_return_task
            >> finish_pre_total_return_task
            >> aave_usdc_sensor
        )
        (
            transform_pre_trailing_return_task
            >> finish_pre_trailing_return_task
            >> aave_usdc_sensor
        )
        transform_tvl_task >> aave_usdc_sensor

        (
            start_pipeline_task
            >> transform_gotk_task
            >> finish_gotk_task
            >> [
                transform_tvl_task,
                transform_pre_total_return_task,
                transform_pre_trailing_return_task,
            ]
        )
        (
            # risk
            aave_usdc_sensor
            >> wbtc_sensor
            >> transform_pre_risk_task
            >> finish_pre_risk_task
            >> trigger_load_task
            >> end_pipeline_task
        )
