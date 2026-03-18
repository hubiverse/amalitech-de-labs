"""
Author: Hubert Apana
Date: 2026-03-18

Helper functions for the application. Utils functions are used to perform various operations on data.
"""

import pandas as pd
import numpy as np
import ast

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
    return Path(__file__).parent.parent / "data" / "tmdb_movies.csv"

def save_dataframe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_csv(path, index=False)
    except Exception:
        raise Exception(f"Failed to save dataframe to csv: {path}")

def load_dataframe(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
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
    Parses the input value based on its type to extract specific information associated
    with the given key. If `val` is a list or dictionary, it processes the content based
    on the key. If `val` is a string representation of a list or dictionary, it attempts
    to safely parse it into a Python object using `ast.literal_eval` and then process
    accordingly. If parsing fails or the input does not conform, it returns NaN.

    :param val: The value to be parsed. It can be of type `list`, `dict`, or `str`. If
        a string represents a serialized `list` or `dict`, it will attempt to parse it.
    :param key: The key to extract information from the given input value. Defaults to 'name'.
    :return: Returns a single string composed of the extracted values separated by a
        pipe (`|`) if the input is a list or parsed list. For dictionaries or parsed
        dictionaries, it returns the value associated with the provided key. If the
        input cannot be processed, `np.nan` is returned.
    """
    if isinstance(val, list):
        return "|".join([i[key] for i in val if key in i])

    if isinstance(val, dict):
        return val.get(key, np.nan)

    if isinstance(val, str):
        if val == "[]" or val == "{}":
            return np.nan
        try:
            # Convert string representation of list/dict to a python object
            data = ast.literal_eval(val)
            if isinstance(data, list):
                return "|".join([i[key] for i in data if key in i])
            if isinstance(data, dict):
                return data.get(key, np.nan)
        except (ValueError, SyntaxError):
            return np.nan

    return np.nan

def extract_director(crew_list):
    """
    Extracts the names of directors from a crew list.

    This function parses the input `crew_list` to identify and extract the names of
    individuals whose job is marked as "Director". The resulting names are concatenated
    into a single string, separated by the "|" symbol. If no directors are found or input
    is invalid, the function returns `numpy.nan`. Both list and string representations
    of the crew data are supported.

    :param crew_list: A list or string representation of crew details containing dictionaries
        with at least 'name' and 'job' keys.
    :return: A string of director names joined by the "|" symbol, or `numpy.nan` if no
        directors are found or the input is invalid.
    """
    if isinstance(crew_list, list):
        directors = [m['name'] for m in crew_list if m.get('job').lower() == 'director']
        return "|".join(directors) if directors else np.nan

    elif isinstance(crew_list, str):
        data = ast.literal_eval(crew_list)
        if isinstance(data, list):
            directors = [m['name'] for m in data if m.get('job').lower() == 'director']
            return "|".join(directors) if directors else np.nan

    return np.nan


def extract_cast(cast_list):
    """
    Extracts and formats a list of cast member names into a single string separated by '|'.

    This function processes input data expected to represent a list of cast members, either
    in Python list format or as a string representation. The function will return a formatted string
    with cast member names concatenated using the '|' symbol. If input data is invalid or does not
    match the expected structure, the return value will be `numpy.nan`.

    :param cast_list: The input data representing cast members. It can be a list of dictionaries where
                      each dictionary contains a 'name' key, or a string representation of such a list.
    :returns: A string with the concatenated cast member names separated by '|', or `numpy.nan` if the
              input is invalid.
    """
    if isinstance(cast_list, list):
        return "|".join([m['name'] for m in cast_list])

    elif isinstance(cast_list, str):
        data = ast.literal_eval(cast_list)
        if isinstance(data, list):
            return "|".join([m['name'] for m in data])

    return np.nan


def safe_len(value):
    """
    Compute the length of the input value safely.

    This function determines the length of the input value based on its type. It works
    with strings, lists, dictionaries, and handles NaN values specifically. If the input
    is a string, it attempts to evaluate it as a literal and compute the length of the
    resulting object if it's a list or dictionary. Otherwise, it returns NaN.

    :param value: Input value, which can be a string, list, dictionary, or another type.
    :type value: Any
    :return: The computed length of the input value if applicable, or NaN otherwise.
    :rtype: int or float
    """
    if isinstance(value, (list, dict)):
        return len(value)

    if isinstance(value, str):
        data = ast.literal_eval(value)
        if isinstance(data, (list, dict)):
            return len(data)
        return len(value)

    return np.nan