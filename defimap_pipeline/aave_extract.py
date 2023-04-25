import json
import random
import pandas as pd
import time
import pendulum
import datetime
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery
from google.cloud import storage
from pendulum import DateTime
from web3 import Web3

### import variables
ALCHEMY_API_KEY = Variable.get("alchemy-api-key")
ETHERSCAN_API_KEY = Variable.get("etherscan-api-key")
AAVE_STRATEGIES = Variable.get("aave-strategies", deserialize_json=True)

### setup variables
CACHE_CONTRACT_ABI = {"atoken": {}}

# gcp variables
DATA_LAKE_BUCKET_NAME = "defimap-data-lake"
CONTRACT_ABI_BUCKET_NAME = "defimap-contract-abi"
GCS_CSV_PATH = f"aave/supply"
RAW_DATASET_NAME = "defimap.raw_data"

# constant
RAY = 10**27
WEI_DECIMALS = 10**18
SECONDS_PER_YEAR = 31536000
# for finding implementation contract address from proxy contract address
IMPLEMENTATION_SLOT = (
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
)
MAXIMUM_BACKOFF = 64  # seconds
MAX_RETRY = 13  # 8 for exponential backoff till max backoff time, another 5 for max backoff time
EXEC_DATE = "{{ ds }}"

RAW_SCHEMA = {
    "date": "DATE",
    "name": "STRING",
    "stake_apy": "FLOAT64",
    "aave_apy": "FLOAT64",
    "total_apy": "FLOAT64",
    "liquidity_index": "FLOAT64",
    "atoken_supply": "FLOAT64",
    "asset_price": "FLOAT64",
    "aave_price": "FLOAT64",
}

# due to compound comptroller contract implemented a PROXY design
AAVE_INCENTIVES_ADDRESS = "0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5"
AAVE_INCENTIVES_DEPLOYED_DATE = "2021-04-26"
AAVE_INCENTIVES_ABI_ADDRESS = "0xD9ED413bCF58c266F95fE6BA63B13cf79299CE31"

AAVE_LENDINGPOOL_ADDRESS = "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"
AAVE_LENDINGPOOL_DEPLOYED_DATE = "2020-12-01"
AAVE_LENDINGPOOL_ABI_ADDRESS = "0xc6845a5c768bf8d7681249f8927877efda425baf"

TOKEN_PRICE_DATASET = "defimap.token_price"


### helper functions
def log(msg: str):
    print(msg)


def random_number_milliseconds():
    return random.random()


"""
TODO: for exponential backoff by attempt number n
@param n: attempt number
"""


def exponential_backoff(n: int):
    time.sleep(min(((2 ** (n - 1)) + random_number_milliseconds()), MAXIMUM_BACKOFF))


"""
TODO: wrapper for calling contract with exponential backoff
Usage: contract = w3.eth.contract(address, abi)
       result = call_contract(block)(contract.functions.function_name)(arg1, arg2)
No argument: call_contract(block)(contract.functions.function_name)()
"""


def call_contract(block):
    def Inner(func):
        def wrapper(*args):
            n_trial = 1
            while n_trial <= MAX_RETRY:
                try:
                    # call the contract here
                    data = func(*args).call(block_identifier=block)
                    return data

                # Other cases
                except Exception as e:
                    log(f"{func.fn_name} Exception: {e}")
                    exponential_backoff(n_trial)
                    n_trial += 1

            raise Exception(f"{func.fn_name} max retry exceeded (block: {block}")

        return wrapper

    return Inner


"""
TODO: connect to web3 via provider
@param provider_api_key: provider api key
"""


def connect_web3_provider(
    provider_api_key: str,
):
    w3 = Web3(
        Web3.HTTPProvider(
            provider_api_key,
            request_kwargs={
                "timeout": 300,
            },
        )
    )
    return w3


"""
TODO: get blocknumber by timestamp
@param timestamp: unix timestamp
"""


def get_blocknumber(
    timestamp: int,
):
    fail_count = 0
    while fail_count < 5:
        res = json.loads(
            requests.get(
                f"https://api.etherscan.io/api",
                params={
                    "module": "block",
                    "action": "getblocknobytime",
                    "timestamp": timestamp,
                    "closest": "after",
                    "apikey": ETHERSCAN_API_KEY,
                },
            ).text
        )
        if res["status"] == "1":
            return int(res["result"])
        else:
            fail_count += 1
            time.sleep(5)

    raise Exception(res["message"])


### contract abi getter
def get_lendingpool_abi():
    try:
        if CACHE_CONTRACT_ABI["lendingpool"]:
            return CACHE_CONTRACT_ABI["lendingpool"]
    except KeyError:
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(CONTRACT_ABI_BUCKET_NAME)
            blob = bucket.blob(f"aave/lendingpool.json")
            contract_abi = json.loads(blob.download_as_string())
            storage_client.close()
            CACHE_CONTRACT_ABI["lendingpool"] = contract_abi
            return contract_abi

        except Exception:
            result = json.loads(
                requests.get(
                    f"https://api.etherscan.io/api",
                    params={
                        "module": "contract",
                        "action": "getabi",
                        "address": AAVE_LENDINGPOOL_ABI_ADDRESS,
                        "apikey": ETHERSCAN_API_KEY,
                    },
                ).text
            )
            if result["status"] == "1":
                contract_abi = str(result["result"])
                storage_client = storage.Client()
                bucket = storage_client.bucket(CONTRACT_ABI_BUCKET_NAME)
                blob = bucket.blob(f"aave/lendingpool.json")
                blob.upload_from_string(contract_abi)
                storage_client.close()
                CACHE_CONTRACT_ABI["lendingpool"] = contract_abi
                return contract_abi

            raise Exception(result["message"])


def get_incentive_abi():
    try:
        if CACHE_CONTRACT_ABI["incentive"]:
            return CACHE_CONTRACT_ABI["incentive"]
    except KeyError:
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(CONTRACT_ABI_BUCKET_NAME)
            blob = bucket.blob(f"aave/incentive.json")
            contract_abi = json.loads(blob.download_as_string())
            storage_client.close()
            CACHE_CONTRACT_ABI["incentive"] = contract_abi
            return contract_abi

        except Exception:
            result = json.loads(
                requests.get(
                    f"https://api.etherscan.io/api",
                    params={
                        "module": "contract",
                        "action": "getabi",
                        "address": AAVE_INCENTIVES_ABI_ADDRESS,
                        "apikey": ETHERSCAN_API_KEY,
                    },
                ).text
            )
            if result["status"] == "1":
                contract_abi = str(result["result"])
                storage_client = storage.Client()
                bucket = storage_client.bucket(CONTRACT_ABI_BUCKET_NAME)
                blob = bucket.blob(f"aave/incentive.json")
                blob.upload_from_string(contract_abi)
                storage_client.close()
                CACHE_CONTRACT_ABI["incentive"] = contract_abi
                return contract_abi

            raise Exception(result["message"])


"""
TODO: get atoken abi by atoken address and name
@param w3: Web3 instance
@param token_key: token name
@param atoken_addr: address of atoken
"""


def get_atoken_abi(
    w3: Web3,
    token_key: str,
    atoken_addr: str,
):
    try:
        # read contract abi from cache
        return CACHE_CONTRACT_ABI["atoken"][token_key]

    except KeyError:
        try:
            # read contract abi from gcs
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(CONTRACT_ABI_BUCKET_NAME)

            blob = bucket.blob(f"aave/atoken/{token_key.upper()}.json")
            contract_abi = json.loads(blob.download_as_string())
            storage_client.close()

            # set contract abi to cache
            CACHE_CONTRACT_ABI["atoken"][token_key] = contract_abi
            # return contract abi
            return contract_abi

        except Exception:
            # read contract abi from etherscan

            contract_address = w3.toChecksumAddress(
                "0x"
                + w3.eth.get_storage_at(atoken_addr, IMPLEMENTATION_SLOT).hex()[26:]
            )

            result = json.loads(
                requests.get(
                    f"https://api.etherscan.io/api",
                    params={
                        "module": "contract",
                        "action": "getabi",
                        "address": contract_address,
                        "apikey": ETHERSCAN_API_KEY,
                    },
                ).text
            )

            if result["status"] == "1":
                contract_abi = str(result["result"])

                storage_client = storage.Client()
                bucket = storage_client.bucket(CONTRACT_ABI_BUCKET_NAME)

                blob = bucket.blob(f"aave/atoken/{token_key.upper()}.json")
                blob.upload_from_string(contract_abi)
                storage_client.close()

                CACHE_CONTRACT_ABI["atoken"][token_key] = contract_abi
                # return contract abi
                return contract_abi

            raise Exception(result["message"])


### getting data
"""
TODO: get stake apy
@param w3: Web3 instance
@param block: block number
@param token_addr: address of underlying token
"""


def get_stake_apy(w3: Web3, block: int, token_addr: str):
    # Get the contract instance
    contract = w3.eth.contract(
        address=AAVE_LENDINGPOOL_ADDRESS, abi=get_lendingpool_abi()
    )
    # Get the apr
    result = call_contract(block)(contract.functions.getReserveData)(token_addr)
    liquidity_rate = result[3]
    apr = liquidity_rate / RAY

    # Calculate the APY
    apy = 100 * (((1 + (apr / SECONDS_PER_YEAR)) ** SECONDS_PER_YEAR) - 1)

    # Return the APY rounded to 3 decimal places
    return round(apy, 3)


"""
TODO: get token price at the given block
@param client: bigQuery Client object
@param token_key: token name
@param start_date: start date in format YYYY-MM-DD
@param target_date: target date in format YYYY-MM-DD
"""


def get_token_price(client, token_key: str, start_date: str, target_date: str):
    # Get the token price
    query = f"""--sql
        SELECT `price_usd` AS `price`, `date` 
        FROM `{TOKEN_PRICE_DATASET}.{token_key.lower()}` 
        WHERE `date` >= '{start_date}'
        AND `date` <= '{target_date}'
    """
    result = client.query(query).to_dataframe()
    result["date"] = result["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    # Return the token price
    return result


"""
TODO: get aave token distribution end timestamp
@param w3: Web3 instance
@param block: block number
"""


def get_aave_distribution_end_date(w3: Web3, block: int):
    contract = w3.eth.contract(
        address=AAVE_INCENTIVES_ADDRESS,
        abi=get_incentive_abi(),
    )
    distribution_end_timestamp = call_contract(block)(
        contract.functions.getDistributionEnd
    )()
    distribution_end_date = datetime.datetime.fromtimestamp(
        distribution_end_timestamp
    ).strftime("%Y-%m-%d")
    return distribution_end_date


"""
TODO: get aave token distribution rate at the given block
@param w3: Web3 instance
@param token_key: token name
@param block: block number
@param asset_price: USD price of ctoken asset 
@param aave_price: USD price of AAVE
@param atoken_supply: atoken supply
@param atoken_addr: address of atoken
"""


def get_aave_apy(
    w3: Web3,
    token_key: str,
    block: int,
    asset_price: float,
    aave_price: float,
    atoken_supply: float,
    atoken_addr: str,
):
    contract = w3.eth.contract(
        address=AAVE_INCENTIVES_ADDRESS,
        abi=get_incentive_abi(),
    )

    result = call_contract(block)(contract.functions.getAssetData)(atoken_addr)
    a_emission_per_second = result[1]

    if a_emission_per_second == 0:
        return float(0)

    underlying_token_decimals = 1e18

    a_emission_per_year = a_emission_per_second * SECONDS_PER_YEAR

    aave_apr = (a_emission_per_year * aave_price * 100) / (
        atoken_supply * asset_price * underlying_token_decimals
    )

    aave_apy = ((1 + (aave_apr / SECONDS_PER_YEAR)) ** SECONDS_PER_YEAR) - 1

    return round(aave_apy, 3)


"""
TODO: get liquidity index to determine atoken increase over timne
@param w3: Web3 instance
@param block: block number
@param token_addr: address of underlying token
"""


def get_liquidity_index(w3: Web3, block: int, token_addr: str):
    contract = w3.eth.contract(
        address=AAVE_LENDINGPOOL_ADDRESS,
        abi=get_lendingpool_abi(),
    )
    result = call_contract(block)(contract.functions.getReserveData)(token_addr)
    liquidity_index = result[1] / RAY

    return liquidity_index


"""
TODO: get total atoken supply
@param w3: Web3 instance
@param token_key: token name
@param block: block number
@param atoken_addr: address of atoken
"""


def get_atoken_supply(
    w3: Web3, client: bigquery.Client, token_key: str, block: int, atoken_addr: str
):
    contract = w3.eth.contract(
        address=atoken_addr,
        abi=get_atoken_abi(client, token_key, atoken_addr),
    )

    total_atoken_supply = call_contract(block)(contract.functions.totalSupply)()
    decimals = contract.functions.decimals().call(block_identifier=block)
    return total_atoken_supply / 10**decimals


"""
TODO: get total apy at the given block
@param stake_apy: stake apy at the given block
@param aave_apy: aave apy at the given block
"""


def get_total_apy(stake_apy: float, aave_apy: float):
    stake_apy = stake_apy or 0
    aave_apy = aave_apy or 0
    total_apy = stake_apy + aave_apy

    return round(total_apy, 3)


### DAG functions

"""
TODO: get daily block number that related to the token
@param start_date: start date
@param target_date: target date
"""


def get_atoken_daily_blocknumber(start_date: DateTime, target_date: DateTime):
    res_list = list()

    while start_date <= target_date:
        block = get_blocknumber(start_date.int_timestamp)
        data = [start_date.format("YYYY-MM-DD"), block]
        res_list.append(data)
        start_date = start_date.add(days=1)

    daily_blocknumber_df = pd.DataFrame(res_list, columns=["date", "block"])
    return daily_blocknumber_df


"""
TODO: extract data from blockchain and upload to GCS
@param token_key: token name
@param token_addr: address of the underlying token
@param atoken_addr: address of atoken
@param atoken_deployed_date: deployed date of atoken (YYYY-MM-DD)
"""


def extract_and_upload_to_gcs(
    token_key: str,
    token_addr: str,
    atoken_addr: str,
    atoken_deployed_date: str,
    **kwargs,
):
    log(f"begin task at: {pendulum.now()}")
    log(kwargs["ds"])
    log(f"{token_key}: begin get daily blocknumber")

    w3 = connect_web3_provider(ALCHEMY_API_KEY)

    client = bigquery.Client()

    # if this dag has not been run before, fetch data from the beginning
    # else, fetch data from the previous execution date
    if not kwargs["prev_data_interval_end_success"]:
        result = client.query(
            f"""--sql
            SELECT MIN(`date`) AS `start_date` FROM `{TOKEN_PRICE_DATASET}.{token_key}`
        """
        )

        price_start_date = pendulum.from_format(
            list(result)[0]["start_date"].strftime("%Y-%m-%d"),
            "YYYY-MM-DD",
        )

        start = max(atoken_deployed_date, price_start_date)

    else:
        start = pendulum.from_format(kwargs["ds"], "YYYY-MM-DD")
    target = pendulum.from_format(
        kwargs["ds"],
        "YYYY-MM-DD",
    )

    daily_blocknumber_df = get_atoken_daily_blocknumber(
        start_date=start, target_date=target
    )

    log(f"{token_key}: get daily blocknumber completed")
    log(f"{token_key}: begin processing data")

    result_list = list()

    aave_price_df = get_token_price(
        client=client,
        token_key="AAVE",
        start_date=start.to_date_string(),
        target_date=target.to_date_string(),
    )

    asset_price_df = get_token_price(
        client=client,
        token_key=token_key,
        start_date=start.to_date_string(),
        target_date=target.to_date_string(),
    )

    last_block = get_blocknumber(target.int_timestamp)
    distribution_end_date = get_aave_distribution_end_date(w3, last_block)

    for _, row in daily_blocknumber_df.iterrows():
        ### get data from blockchain
        stake_apy = get_stake_apy(
            w3,
            block=row["block"],
            token_addr=token_addr,
        )

        asset_price = asset_price_df.loc[
            asset_price_df["date"] == row["date"],
            "price",
        ].squeeze()

        aave_price = aave_price_df.loc[
            aave_price_df["date"] == row["date"],
            "price",
        ].squeeze()

        liquidity_index = get_liquidity_index(
            w3,
            block=row["block"],
            token_addr=token_addr,
        )

        atoken_supply = get_atoken_supply(
            w3,
            client,
            token_key=token_key,
            block=row["block"],
            atoken_addr=atoken_addr,
        )

        aave_apy = (
            get_aave_apy(
                w3,
                token_key=token_key,
                block=row["block"],
                asset_price=asset_price,
                aave_price=aave_price,
                atoken_supply=atoken_supply,
                atoken_addr=atoken_addr,
            )
            if row["date"] > AAVE_INCENTIVES_DEPLOYED_DATE
            and row["date"] < distribution_end_date
            else 0
        )

        total_apy = get_total_apy(stake_apy, aave_apy)

        result_list.append(
            [
                row["date"],
                token_key,
                stake_apy,
                aave_apy,
                total_apy,
                liquidity_index,
                atoken_supply,
                asset_price,
                aave_price,
            ]
        )
    df = pd.DataFrame(
        result_list,
        columns=[
            "date",
            "name",
            "stake_apy",
            "aave_apy",
            "total_apy",
            "liquidity_index",
            "atoken_supply",
            "asset_price",
            "aave_price",
        ],
    )

    # upload *RAW* .csv file to gcs
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(DATA_LAKE_BUCKET_NAME)
    blob = bucket.blob(f"{GCS_CSV_PATH}/{token_key}/{kwargs['ds']}.csv")
    blob.upload_from_string(df.to_csv(index=False), "text/csv")
    storage_client.close()
    log(f"{token_key}: load data to gcs completed")
    log(f"{token_key}: task complete")


"""
TODO: read & preprocess raw data, then load to bq
@param token_key: token name
@param path: path to the .csv file on gcs (ex. "gs://data-lake/.../file.csv")
"""


def load_to_bq(token_key: str, path: str, **kwargs):
    # read from gcs
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])

    # fill missing
    FILL_ZERO_COLS = ["aave_apy", "liquidity_index"]
    INTERPOLATE_COLS = [
        col_name
        for col_name, col_dtype in RAW_SCHEMA.items()
        if (col_name not in FILL_ZERO_COLS) and (col_dtype == "FLOAT64")
    ]
    df[FILL_ZERO_COLS] = df[FILL_ZERO_COLS].fillna(value=0.0)
    df[INTERPOLATE_COLS] = df[INTERPOLATE_COLS].interpolate(
        method="linear", limit_direction="forward", inplace=False
    )
    min_date = df["date"].min().strftime("%Y-%m-%d")
    max_date = df["date"].max().strftime("%Y-%m-%d")
    # load to bq
    client = bigquery.Client()
    raw_table_name = f"{RAW_DATASET_NAME}.aave_{token_key}"

    # delete existing table
    if not kwargs["prev_data_interval_end_success"]:
        print("Running for the first time, clearing existing table")
        client.delete_table(raw_table_name, not_found_ok=True)
        print(f"Deleted existing table: {raw_table_name}")
    else:
        query = f"""--sql
            DELETE
            FROM
                `{raw_table_name}`
            WHERE
                `date` <= '{max_date}'
            AND
                `date` >= '{min_date}'
        """
        client.query(query)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(col_name, col_dtype)
            for col_name, col_dtype in RAW_SCHEMA.items()
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, raw_table_name, job_config=job_config)

    result = job.result()
    if result.state.lower() == "done":
        print("Loaded to big query {}".format(result.state.lower()))
    client.close()


### DAG
def create_dag(
    dag_id: str,
    description: str,
    schedule_interval: str,
    start_date: DateTime,
    tags: list,
):
    dag = DAG(
        dag_id=dag_id,
        default_args={
            "retries": 5,
        },
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=True,
        tags=tags,
        max_active_runs=1,
        concurrency=5,
    )

    start_pipeline_task = EmptyOperator(
        task_id="start_pipeline",
        dag=dag,
        # depends_on_past=True,
        # wait_for_downstream=True,
    )

    price_sensor_aave = ExternalTaskSensor(
        task_id=f"price_sensor_aave",
        dag=dag,
        external_dag_id="common_extract_price",
        external_task_ids=[f"load_to_bq_aave"],
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=2 * 60,  # 2 minutes
        timeout=30 * 60,  # 30 mins
        # execution_delta=timedelta(minutes=30),
    )

    trigger_transform_task = TriggerDagRunOperator(
        dag=dag,
        task_id="trigger_transform",
        trigger_dag_id=f"aave_transform",
        reset_dag_run=True,
        # wait_for_completion=True,
        execution_date=EXEC_DATE,
        allowed_states=["success", "skipped"],
    )

    end_pipeline_task = EmptyOperator(
        task_id="end_pipeline",
        dag=dag,
    )

    for token_key, value in AAVE_STRATEGIES.items():
        token_addr = value["token_address"]
        atoken_addr = value["atoken_address_for_assets"]
        atoken_deployed_date = pendulum.from_format(
            value["atoken_deployed_date"], "YYYY-MM-DD", tz="UTC"
        )

        # Wait for token price to be ready
        if token_key != "aave":
            # aave price already has a price sensor
            price_sensor = ExternalTaskSensor(
                task_id=f"price_sensor_{token_key}",
                dag=dag,
                external_dag_id="common_extract_price",
                external_task_ids=[f"load_to_bq_{token_key}"],
                allowed_states=["success"],
                mode="reschedule",
                poke_interval=2 * 60,  # 2 minutes
                timeout=30 * 60,  # 30 mins
                # execution_delta=timedelta(minutes=30),
            )

        # Extract & upload to gcs
        extract_and_upload_to_gcs_task = PythonOperator(
            dag=dag,
            task_id=f"extract_{token_key}",
            python_callable=extract_and_upload_to_gcs,
            provide_context=True,
            retries=5,
            retry_delay=pendulum.duration(minutes=3),
            op_kwargs={
                "token_key": token_key,
                "token_addr": token_addr,
                "atoken_addr": atoken_addr,
                "atoken_deployed_date": atoken_deployed_date,
            },
        )

        # Load to BigQuery
        load_to_bq_task = PythonOperator(
            dag=dag,
            task_id=f"load_to_bq_{token_key}",
            python_callable=load_to_bq,
            provide_context=True,
            retries=5,
            retry_delay=pendulum.duration(minutes=3),
            op_kwargs={
                "token_key": token_key,
                "path": f"gs://{DATA_LAKE_BUCKET_NAME}/{GCS_CSV_PATH}/{token_key}/{EXEC_DATE}.csv",
            },
        )

        # Define subtask dependency
        (
            start_pipeline_task
            >> price_sensor_aave
            >> extract_and_upload_to_gcs_task
            >> load_to_bq_task
            >> trigger_transform_task
            >> end_pipeline_task
        )

        # aave already have a price sensor; define extra dependency
        if token_key != "aave":
            start_pipeline_task >> price_sensor >> extract_and_upload_to_gcs_task

    return dag


## Main DAG declaration ##

globals()["aave_extract"] = create_dag(
    dag_id="aave_extract",
    description="Extract AAVE Supply",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 3, 20, 0, 0, 0, tz="UTC"),
    tags=["aave", "extract"],
)
