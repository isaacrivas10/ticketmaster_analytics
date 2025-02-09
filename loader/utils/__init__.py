import json
import numpy as np
import pandas as pd
from typing import Any
from pathlib import Path
from .google_cloud import (
    upload_dataframe_to_gcs,
    list_new_parquet_files,
    load_parquet_to_bigquery,
)


def replace_dots_in_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces dots in the column names of a pandas DataFrame with underscores.

    :param df: DataFrame whose column names need to be modified
    :return: DataFrame with modified column names
    """
    df.columns = df.columns.str.replace(".", "_", regex=False)
    return df


def filter_dicts(
    dict_list: list[dict[str, Any]], keys: list[str]
) -> list[dict[str, Any]]:
    """
    Filters a list of dictionaries to include only the specified keys.

    :param dict_list: List of dictionaries to filter
    :param keys: List of keys to include in the filtered dictionaries
    :return: New list of dictionaries containing only the specified keys
    """
    return [{k: d[k] for k in keys if k in d} for d in dict_list]


def save_latest_timestamp(timestamp: str, path: str) -> None:
    """
    Saves the latest timestamp to a JSON file.

    :param timestamp: Timestamp to save
    :param path: Path to the JSON file
    """
    TIMESTAMP_FILE = Path(path)
    if TIMESTAMP_FILE.exists():
        with open(TIMESTAMP_FILE, "r") as file:
            data = json.load(file)
    else:
        data = []

    data.append({"timestamp": timestamp})

    with open(TIMESTAMP_FILE, "w+") as file:
        json.dump(data, file, indent=4)


def load_latest_timestamp(path: str, default: str) -> str:
    """
    Loads the latest timestamp from a JSON file.

    :param path: Path to the JSON file
    :param default: String to return if the file does not exist or is empty
    :return: Latest timestamp in the file
    """
    TIMESTAMP_FILE = Path(path)
    if TIMESTAMP_FILE.exists():
        with open(TIMESTAMP_FILE, "r") as file:
            data = json.load(file)
            if data:
                return data[-1]["timestamp"]
    return default


def lowercase_keys(obj: np.dtypes.ObjectDType) -> Any:
    """
    Recursively converts all dictionary keys to lowercase, including nested arrays.

    :param obj: Dictionary or list to convert
    :return: Dictionary or list with lowercase keys
    """
    if isinstance(obj, dict):
        return {k.lower(): lowercase_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [lowercase_keys(v) if isinstance(v, (dict, list)) else v for v in obj]
    else:
        return obj


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies transformations to clean column names and standardize JSON fields.

    :param df: Input DataFrame
    :return: Transformed DataFrame
    """
    df = replace_dots_in_column_names(df)

    # Apply lowercase transformation to JSON fields
    # Pandas and PyArrow allow for case insensitive column names so we can use lowercase keys for JSON fields to avoid having duplicated columns in BigQuery
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
                df[col] = df[col].apply(
                    lowercase_keys
                )  # Standardize keys to lowercase recursively
            except (json.JSONDecodeError, TypeError):
                continue  # Ignore non-JSON columns
    return df


event_keys = [
    "name",
    "type",
    "id",
    "locale",
    "sales",
    "dates",
    "info",
    "classifications",
    "promoter",
    "promoters",
    "priceRanges",
    "products",
    "accessibility",
    "location",
    "units",
    "description",
    "ageRestrictions",
    "ticketing",
    "_embedded",
]

all = [
    event_keys,
    filter_dicts,
    save_latest_timestamp,
    load_latest_timestamp,
    upload_dataframe_to_gcs,
    list_new_parquet_files,
    load_parquet_to_bigquery,
    process_dataframe,
]
