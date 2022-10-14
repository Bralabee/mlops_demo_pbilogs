# import python functions to build pipeline
# from first_ingestion_point import start_pipeline_pbia, \
#     format_datatype_dates, add_month_year_cols_users, api_calls_filtered_out, lower_case_cols

from process_data_module import load_concat_file, start_pipeline_pbia,  \
    format_datatype_dates, api_calls_filtered_out, \
    lower_case_cols, log_step_pbia  # , save_data

# import config from helper.py
from helper import load_config

import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
from functools import wraps

import datetime as dt
import glob
from datetime import datetime, timedelta, timezone

# variables for the 4th transformation node i.e. api_calls_filtered_out
service_account = 'SA_DAL_PowerBI@hs2.org.uk'
operation = 'ExportActivityEvents'
today = datetime.today()
now = datetime.now(timezone.utc)


@log_step_pbia
@task(name="Start First Transformation Pipeline")
def start_transformations_pbia(config, df) -> pd.DataFrame:
    return (
        df.pipe(start_pipeline_pbia)
        .pipe(lower_case_cols, 'UserId', 'ReportName')
        .pipe(api_calls_filtered_out, service_account, operation)
        .pipe(format_datatype_dates, config)
        #.pipe(add_month_year_cols_users, 'CreationTime')
    )


@flow(name="Power BI Activity Log Script")
def pbia_flow():
    config = load_config()
    df = load_concat_file(config)
    start_transformations_pbia(config, df)

    #start_transformations_pbia(config, df)

    # save_data(df, config)


if __name__ == "__main__":
    pbia_flow()
