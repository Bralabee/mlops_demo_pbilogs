# useful libraries
import glob
import pandas as pd
import datetime as dt
from functools import wraps
from prefect import flow, task
# import config from helper.py
from helper import load_config
from omegaconf import DictConfig
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
from sklearn.model_selection import train_test_split

# variables for the 4th transformation node i.e. api_calls_filtered_out
service_account = 'SA_DAL_PowerBI@hs2.org.uk'
operation = 'ExportActivityEvents'
today = datetime.today()
now = datetime.now(timezone.utc)

# nodes for the first ingestion point
from process_data_module import load_concat_file, start_pipeline_pbia, format_datatype_dates, api_calls_filtered_out, \
    lower_case_cols, log_step_pbia

# nodes for the second ingestion point
from process_data_module import add_month_year_cols_users


@log_step_pbia
@task(name="Start First Transformation Pipeline")
def start_transformations_pbia(config, df) -> pd.DataFrame:
    return (
        df.pipe(start_pipeline_pbia)
        .pipe(lower_case_cols, 'UserId', 'ReportName')
        .pipe(api_calls_filtered_out, service_account, operation)
        .pipe(format_datatype_dates, config)
        # .pipe(add_month_year_cols_users, 'CreationTime')
    )


@log_step_pbia
@task(name="Start Second Transformation Pipeline")
def start_transformations_pbia_2(df) -> pd.DataFrame:
    return (
        df.pipe(add_month_year_cols_users, 'CreationTime')
        # .pipe(lower_case_cols, 'UserId', 'ReportName')
        # .pipe(api_calls_filtered_out, service_account, operation)
        # .pipe(format_datatype_dates, config)
        # .pipe(add_month_year_cols_users, 'CreationTime')
    )


@flow(name="Power BI Activity Log Script")
def pbia_flow():
    config = load_config()
    df = load_concat_file(config)
    df = start_transformations_pbia(config, df)
    start_transformations_pbia_2(df)

    # start_transformations_pbia(config, df)

    # save_data(df, config)


if __name__ == "__main__":
    pbia_flow()
