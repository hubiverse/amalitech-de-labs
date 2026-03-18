"""
Author: Hubert Apana
Date: 2026-03-18

A set of utility functions for downloading and managing movie data using the TMDB API,
with retry logic for transient errors.

The module includes functionality for fetching individual movies by ID, downloading a list
of movies, and managing cached movie data using a pandas DataFrame.
"""

import httpx
import pandas as pd
import logging
from typing import List, Tuple, Optional
from tqdm import tqdm
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    RetryError
)
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

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TMDB_Pipeline")

#  CUSTOM EXCEPTIONS
class TMDBHardError(Exception): """4xx errors except 429 - do not retry."""
class TMDBSoftError(Exception): """5xx or 429 errors - retryable."""


def fetch_movie_by_id(
        client: httpx.Client,
        settings: Settings,
        movie_id: int,
        max_retries: int = 3,
        wait_factor: float = 2.0
) -> Optional[Movie]:
    """
    Fetches a movie by its ID from the TMDb API with retry logic for handling transient issues.

    Retries are implemented using an exponential backoff strategy. Soft errors (e.g., status
    codes 429, 500-599) trigger retries up to the `max_retries` limit. Hard errors (e.g.,
    status codes 400-499 or validation issues) abort the process. The fetched data is parsed
    and validated against the `Movie` model.

    :param client: Instance of `httpx.Client` used to make HTTP requests to the TMDb API.
    :param settings: Configuration object containing TMDb API settings such as base URL
        and access token.
    :param movie_id: Unique identifier of the movie to fetch from the TMDb API.
    :param max_retries: Maximum number of retry attempts for handling soft errors.
    :param wait_factor: Exponential backoff factor defining the wait time between retries.
    :return: A `Movie` instance validated against the `Movie` model if successful, or `None`
        in case of failure after retries.
    """

    # stop_after_attempt(1) means 1 total attempt (0 retries)
    @retry(
        retry=retry_if_exception_type(TMDBSoftError),
        stop=stop_after_attempt(max_retries + 1),
        wait=wait_exponential(multiplier=wait_factor, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    def _attempt_fetch():
        url = movie_url(settings.tmdb_api_base_url, movie_id)
        params = {"append_to_response": "credits"}

        response = client.get(
            url,
            headers=auth_headers(settings.tmdb_api_access_token),
            params=params
        )

        status = response.status_code
        if status == 429 or 500 <= status < 600:
            logger.warning(f"Soft error {status} for ID {movie_id}. Retrying...")
            raise TMDBSoftError(f"Status {status}")

        if 400 <= status < 500:
            logger.error(f"Hard error {status} for ID {movie_id}. Skipping.")
            raise TMDBHardError(f"Status {status}")

        response.raise_for_status()
        data = response.json()

        try:
            # Map the appended credits if they exist
            if "credits" in data:
                data["cast"] = data["credits"].get("cast", [])
                data["crew"] = data["credits"].get("crew", [])

            # Model validation
            movie = Movie.model_validate(data)

            return movie
        except Exception as e:
            logger.error(f"Schema validation error for ID {movie_id}: {e}")
            raise TMDBHardError("Pydantic Validation Failed")

    try:
        return _attempt_fetch()
    except (TMDBHardError, RetryError, Exception) as e:
        # Final failure point after all retries
        logger.error(f"Final failure for Movie ID {movie_id}: {str(e)}")
        return None


def download_movies_by_ids(
        settings: Settings,
        movie_ids: Iterable[int],
        max_retries: int = 3,
        wait_factor: float = 2.0
) -> Tuple[List[Movie], List[int]]:
    """
    Downloads movies based on the provided list of movie IDs.

    This function fetches movie details for the given list of movie IDs using HTTP
    requests. It allows for configurable retry logic and waiting periods between
    retries in case of failures. Successfully fetched movies and IDs of failed
    requests are returned separately.

    :param settings: The application settings used for configuration, including
        API keys and endpoints, passed as a `Settings` object.
    :param movie_ids: An iterable containing the IDs of movies to fetch.
    :param max_retries: The maximum number of retries allowed per movie in the
        event of a failure. Defaults to 3.
    :param wait_factor: A multiplier used to determine the waiting time between
        consecutive retries. Defaults to 2.0.
    :return: A tuple containing a list of successfully fetched `Movie` objects
        and a list of movie IDs for which fetching failed.
    """
    valid_movies = []
    failed_ids = []

    # Use a single client for all requests
    with httpx.Client(timeout=15.0) as client:
        for movie_id in tqdm(movie_ids, desc="Fetching Movies"):
            movie = fetch_movie_by_id(
                client=client,
                settings=settings,
                movie_id=movie_id,
                max_retries=max_retries,
                wait_factor=wait_factor
            )

            if movie:
                valid_movies.append(movie)
            else:
                failed_ids.append(movie_id)

    return valid_movies, failed_ids

def get_movies_dataframe_from_ids(
        settings: Settings,
        movie_ids: Iterable[int],
        cache_csv_path: Path | None = None,
        force_redownload: bool = False,
        max_retries: int = 3,
        waite_factor: float = 2.0
) -> Tuple[pd.DataFrame, List[int]]:
    """
    Retrieves movie data in the form of a pandas DataFrame based on the provided list of
    movie IDs. The method uses caching functionality to avoid redundant downloads, supports
    forced redownload, and includes retry logic for robustness. Only the requested subset
    of movie IDs is included in the final DataFrame.

    :param settings: Configuration object providing settings related to the download
                     process and API integration.
    :param movie_ids: Collection of unique movie IDs whose details need to be fetched.
    :param cache_csv_path: Path to the cached CSV file for storing and retrieving movie
                           data. If None, a default path is used.
    :param force_redownload: Whether to ignore existing cached data and force downloading
                             movies for the given IDs.
    :param max_retries: Maximum number of retry attempts for downloading failed movie
                        data.
    :param waite_factor: Multiplier to increase wait time exponentially between retries
                         to improve success rates of downloads.
    :return: A tuple consisting of:
             - A DataFrame containing movie data for the requested IDs.
             - A list of IDs for which data download was unsuccessful.
    :rtype: Tuple[pd.DataFrame, List[int]]
    """
    path = cache_csv_path or default_cache_path()
    requested_ids = list(dict.fromkeys(movie_ids))

    # Loading logic
    cached_df = load_dataframe(path) if path.exists() else pd.DataFrame()

    cached_ids = (
        set(cached_df["id"].astype(int).tolist())
        if not cached_df.empty and "id" in cached_df.columns
        else set()
    )

    ids_to_download = (
        requested_ids if force_redownload
        else [mid for mid in requested_ids if mid not in cached_ids]
    )

    all_failed_ids = []

    if ids_to_download:
        downloaded_movies, failed_ids = download_movies_by_ids(
            settings=settings,
            movie_ids=ids_to_download,
            max_retries=max_retries,
            wait_factor=waite_factor
        )
        all_failed_ids = failed_ids

        if downloaded_movies:
            downloaded_df = to_dataframe(downloaded_movies)
            updated_cache_df = merge_movies_dataframe(
                cached_df=cached_df,
                new_df=downloaded_df,
            )
            save_dataframe(updated_cache_df, path)
            cached_df = updated_cache_df

    # Return only the requested subset from the cache
    final_df = filter_movies_by_ids(movie_df=cached_df, movie_ids=requested_ids)

    return final_df, all_failed_ids