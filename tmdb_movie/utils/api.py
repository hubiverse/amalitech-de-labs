import httpx
import pandas as pd

from pathlib import Path
from typing import Iterable

from config import Settings
from tmdb_movie.utils.types import Movie
from tmdb_movie.utils.helpers import (
    movie_url,
    auth_headers,
    default_cache_path,
    load_dataframe,
    to_dataframe,
    save_dataframe,
    merge_movies_dataframe,
    filter_movies_by_ids
)

def fetch_movie_by_id(client: httpx.Client, settings: Settings, movie_id: int) -> Movie:
    response = client.get(
        movie_url(settings.tmdb_api_base_url, movie_id),
        headers=auth_headers(settings.tmdb_api_access_token),
    )
    response.raise_for_status()
    return Movie.model_validate(response.json())


def download_movies_by_ids(settings: Settings, movie_ids: Iterable[int]) -> list[Movie]:
    with httpx.Client(timeout=10.0) as client:
        return [
            fetch_movie_by_id(client=client, settings=settings, movie_id=movie_id)
            for movie_id in movie_ids
        ]

def get_movies_dataframe_from_ids(
    settings: Settings,
    movie_ids: Iterable[int],
    cache_csv_path: Path | None = None,
    force_redownload: bool = False,
) -> pd.DataFrame:
    path = cache_csv_path or default_cache_path()
    requested_ids = list(dict.fromkeys(movie_ids))

    if path.exists():
        cached_df = load_dataframe(path)
    else:
        cached_df = pd.DataFrame()

    cached_ids = (
        set(cached_df["id"].dropna().astype(int).tolist())
        if not cached_df.empty and "id" in cached_df.columns
        else set()
    )

    ids_to_download = (
        requested_ids
        if force_redownload
        else [movie_id for movie_id in requested_ids if movie_id not in cached_ids]
    )

    if ids_to_download:
        downloaded_movies = download_movies_by_ids(
            settings=settings,
            movie_ids=ids_to_download,
        )
        downloaded_df = to_dataframe(downloaded_movies)

        updated_cache_df = merge_movies_dataframe(
            cached_df=cached_df,
            new_df=downloaded_df,
        )

        save_dataframe(updated_cache_df, path)
    else:
        updated_cache_df = cached_df

    return filter_movies_by_ids(
        movie_df=updated_cache_df,
        movie_ids=requested_ids,
    )