"""
Author: Hubert Apana
Date: 2026-03-18

Helper functions for the application. Utils functions are used to perform various operations on data.
"""

import pandas as pd
import numpy as np

from pathlib import Path
from typing import Iterable

from .types import Movie

def movie_url(base_url: str, movie_id: int) -> str:
    return f"{base_url.rstrip('/')}/movie/{movie_id}"

def auth_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

def default_cache_path() -> Path:
    return Path(__file__).parent.parent / "data" / "tmdb_movies.pkl"

def save_dataframe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_pickle(path)
    except Exception:
        raise Exception(f"Failed to save dataframe to csv: {path}")

def load_dataframe(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_pickle(path)
        except Exception:
            raise Exception(f"Failed to read csv file: {path}")

    raise FileNotFoundError(f"No cached csv file found at {path}")

def to_dataframe(movies: Iterable[Movie]) -> pd.DataFrame:
    """
    Covert movie objects to a dataframe

    :param movies: Iterable of Movie objects to convert to a dataframe
    :return: DataFrame containing the movie data
    """
    rows = [movie.model_dump() for movie in movies]
    return pd.DataFrame(rows)


def merge_movies_dataframe(cached_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes on the basis of 'id' column. The new_df takes precedence over the cached_df in case of duplicates.

    :param cached_df: DataFrame containing the cached movie data from the csv file
    :param new_df: DataFrame containing the new movie data to be merged with the cached data
    :return: Merged DataFrame containing the combined movie data with duplicates removed based on 'id' column
    """
    if cached_df.empty:
        return new_df.copy()

    if new_df.empty:
        return cached_df.copy()

    combined_df = pd.concat([cached_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset="id", keep="last")
    return combined_df


def filter_movies_by_ids(movie_df: pd.DataFrame, movie_ids: list[int]) -> pd.DataFrame:
    """
    Filters a given DataFrame of movies based on a list of movie IDs. If the provided DataFrame
    is empty, a copy of it is returned. Otherwise, the function filters the DataFrame to include
    only rows for the specified movie IDs and preserves the order of IDs as defined in the list.
    The filtered DataFrame is returned with its index reset.

    :param movie_df: A pandas DataFrame containing movie information, which must include a column
        named 'id' to identify movies.
    :param movie_ids: A list of integers representing the IDs of movies to be included in the
        filtered DataFrame.
    :return: A pandas DataFrame containing only the rows corresponding to the specified movie IDs,
        in the same order as the IDs provided in the list. The index of the returned DataFrame is reset.
    """
    if movie_df.empty:
        return movie_df.copy()

    filtered_df = movie_df[movie_df["id"].isin(movie_ids)].copy()

    order_df = pd.DataFrame({"id": movie_ids, "_order": range(len(movie_ids))})
    filtered_df = filtered_df.merge(order_df, on="id", how="inner")
    filtered_df = filtered_df.sort_values("_order").drop(columns="_order")

    return filtered_df.reset_index(drop=True)


def safe_parse(val, key='name'):
    """
    Parses the input value based on its type and extracts information using the specified key.

    The function processes input data which can either be a list or a dictionary. If the input
    value is a list, it concatenates the values corresponding to the specified key in each
    dictionary within the list, using a pipe ``|`` delimiter. If the input is a dictionary,
    it extracts and returns the value associated with the specified key. If the key is not
    found or the input data type is unsupported, ``numpy.nan`` is returned.

    :param val: The input value to be parsed. Can be of type ``list`` or ``dict``.
    :param key: The key to extract data from the input. Defaults to 'name'.
    :return: A concatenated string of values, a single value from the dictionary,
             or ``numpy.nan`` if the key isn't found or an unsupported data type is given.
    """
    if isinstance(val, list):
        return "|".join([i[key] for i in val if key in i])

    if isinstance(val, dict):
        return val.get(key, np.nan)

    return np.nan

def extract_cast(cast_list):
    """
    Extracts and formats the names of cast members from the given list.

    Checks if the input is a list. If true, it extracts the 'name'
    attribute from each dictionary in the list and joins them into a single
    string separated by a vertical bar ('|'). If the input is not a list,
    returns a numpy NaN value.

    :param cast_list: List of dictionaries, where each dictionary represents
        a cast member and contains a 'name' key.
    :type cast_list: list
    :return: A string of concatenated cast member names separated by a
        vertical bar ('|'), or numpy NaN if the input is not a list.
    :rtype: str or float
    """

    if isinstance(cast_list, list):
        return "|".join([m['name'] for m in cast_list])

    return np.nan