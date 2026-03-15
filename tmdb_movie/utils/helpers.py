import pandas as pd

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