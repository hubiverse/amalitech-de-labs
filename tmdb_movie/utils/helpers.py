import pandas as pd
import numpy as np
import ast

from pathlib import Path
from typing import Iterable

from .types import Movie

def movie_url(base_url: str, movie_id: int) -> str:
    return f"{base_url.rstrip('/')}/movie/{movie_id}"

def movie_credits_url(base_url: str, movie_id: int) -> str:
    return f"{movie_url(base_url, movie_id)}/credits"

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
    rows = [movie.model_dump() for movie in movies]
    return pd.DataFrame(rows)


def merge_movies_dataframe(cached_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if cached_df.empty:
        return new_df.copy()

    if new_df.empty:
        return cached_df.copy()

    combined_df = pd.concat([cached_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset="id", keep="last")
    return combined_df


def filter_movies_by_ids(movie_df: pd.DataFrame, movie_ids: list[int]) -> pd.DataFrame:
    if movie_df.empty:
        return movie_df.copy()

    filtered_df = movie_df[movie_df["id"].isin(movie_ids)].copy()

    order_df = pd.DataFrame({"id": movie_ids, "_order": range(len(movie_ids))})
    filtered_df = filtered_df.merge(order_df, on="id", how="inner")
    filtered_df = filtered_df.sort_values("_order").drop(columns="_order")

    return filtered_df.reset_index(drop=True)


def safe_parse(val, key='name'):
    """Parses JSON-like strings and extracts a specific key, joined by '|'."""
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
    if isinstance(crew_list, list):
        directors = [m['name'] for m in crew_list if m.get('job') == 'Director']
        return "|".join(directors) if directors else np.nan

    elif isinstance(crew_list, str):
        data = ast.literal_eval(crew_list)
        if isinstance(data, list):
            directors = [m['name'] for m in data if m.get('job') == 'Director']
            return "|".join(directors) if directors else np.nan

    return np.nan


def extract_cast(cast_list):
    if isinstance(cast_list, list):
        return "|".join([m['name'] for m in cast_list])

    elif isinstance(cast_list, str):
        data = ast.literal_eval(cast_list)
        if isinstance(data, list):
            return "|".join([m['name'] for m in data])

    return np.nan


def safe_len(value):
    if isinstance(value, (list, dict)):
        return len(value)

    if isinstance(value, str):
        data = ast.literal_eval(value)
        if isinstance(data, (list, dict)):
            return len(data)
        return len(value)

    if pd.isna(value):
        return np.nan
    return np.nan