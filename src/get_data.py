import pandas as pd
from omegaconf import DictConfig
from prefect import flow, task
import glob
# from sqlalchemy import create_engine

from helper import load_config


# load raw files and concat into single data frame using columns defined above as column names for the dataframe to
# be created.

@task(name="Load Data")
def load_raw_files(config: DictConfig):
    all_df = pd.concat(
        [pd.read_csv(logs, low_memory=False, header=config.data.raw.skiprows, sep=config.data.raw.delimiter,
                     encoding=config.data.rawdailyconcatinated.encoding)
         for logs in glob.glob(config.data.raw.path)])

    return all_df


@task(name="Save Data")
def save_data(all_df: pd.DataFrame, config: DictConfig):
    all_df.to_csv(config.data.rawdailyconcatinated.path)


@flow(name="get_data")
def get_data():
    config = load_config()
    df = load_raw_files(config)
    save_data(df, config)


if __name__ == "__main__":
    get_data()
