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