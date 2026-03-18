"""
Unit tests for validating TMDb movie data retrieval and cache logic.

These test cases are designed to verify that the `get_movies_dataframe_from_ids`
method behaves as expected under different conditions involving cached data, API calls,
and forced redownloads. Specifically, it ensures that the function respects cache files,
handles API interactions correctly, and can gracefully handle failed requests.

Fixtures:
    mock_settings: A pytest fixture that provides mock configuration data for the
        application, including TMDb API base URL, access token, and API key.

Tests:
    test_cache_logic_avoids_network_call:
        Verifies that no network call is made when a requested movie ID is already
        present in the cache.

    test_cache_partial_hit:
        Ensures that only uncached movie IDs trigger API calls while cached IDs are skipped.

    test_force_redownload_ignores_cache:
        Validates that setting `force_redownload` to True forces API calls even for movie
        IDs present in the cache.

    test_failed_api_returns_ids_and_doesnt_crash:
        Confirms that 404 errors return failed IDs properly in the `failed` list and the
        function does not crash when such scenarios occur.
"""

import pytest
import respx
import pandas as pd
from httpx import Response
from pathlib import Path

from tmdb_movie.utils import get_movies_dataframe_from_ids, movie_url
from config import Settings


@pytest.fixture
def mock_settings():
    return Settings(
        tmdb_api_base_url="https://api.themoviedb.org/3",
        tmdb_api_access_token="fake_token",
        tmdb_api_key="fake_key"
    )

@pytest.fixture
def tmp_path():
    dir_path =  Path(__file__).parent / "data"
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    return dir_path

@respx.mock
def test_cache_logic_avoids_network_call(mock_settings, tmp_path):
    """
    Scenario: ID 19995 is already in the CSV.
    Expectation: No API call is made when requesting ID 19995.
    """
    movie_id = 19995
    # Setup a fake cache file
    cache_file = tmp_path / "cache.csv"
    existing_data = pd.DataFrame({"id": [movie_id], "title": ["Avatar"]})
    existing_data.to_csv(cache_file, index=False)

    # Mock the API
    api_route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(200, json={"id": movie_id, "title": "Avatar"})
    )

    # Run the function
    df, failed = get_movies_dataframe_from_ids(
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_csv_path=cache_file,
        force_redownload=False
    )

    assert api_route.call_count == 0
    assert not df.empty
    assert df.iloc[0]["title"] == "Avatar"


@respx.mock
def test_cache_partial_hit(mock_settings, tmp_path):
    """
    Scenario: ID 1 is cached, ID 2 is not.
    Expectation: Only 1 API call is made (for ID 2).
    """
    movie_1_id = 1
    cache_file = tmp_path / "cache.csv"
    pd.DataFrame({"id": [movie_1_id], "title": ["Movie 1"]}).to_csv(cache_file, index=False)

    # Mock API for movie 2
    movie_2_id = 2
    route_2 = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_2_id)).mock(
        return_value=Response(200, json={"id": movie_2_id, "title": "Movie 2", "status": "Released"})
    )
    # Mock API for movie 1 (should not be called)
    route_1 = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_1_id)).mock(Response(200))

    df, failed = get_movies_dataframe_from_ids(
        settings=mock_settings,
        movie_ids=[movie_1_id, movie_2_id],
        cache_csv_path=cache_file
    )

    assert route_2.call_count == 1
    assert route_1.call_count == 0
    assert len(df) == 2


@respx.mock
def test_force_redownload_ignores_cache(mock_settings, tmp_path):
    """
    Scenario: ID 1 is cached, but force_redownload is True.
    Expectation: API is called despite existing data.
    """
    movie_id = 1
    cache_file = tmp_path / "cache.csv"
    pd.DataFrame({"id": [movie_id], "title": ["Old Title"]}).to_csv(cache_file, index=False)

    api_route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(200, json={"id": movie_id, "title": "New Title", "status": "Released"})
    )

    df, failed = get_movies_dataframe_from_ids(
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_csv_path=cache_file,
        force_redownload=True
    )

    assert api_route.call_count == 1
    assert df.iloc[0]["title"] == "New Title"
    assert df.iloc[0]["id"] == movie_id
    assert len(df) == 1


@respx.mock
def test_failed_api_returns_ids_and_doesnt_crash(mock_settings, tmp_path):
    """
    Scenario: Requesting a movie that doesn't exist (404).
    Expectation: The ID is returned in the 'failed' list.
    """
    cache_file = tmp_path / "no_file.csv"
    movie_id = 404
    respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(return_value=Response(404))

    df, failed = get_movies_dataframe_from_ids(
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_csv_path=cache_file
    )

    assert movie_id in failed
    assert df.empty
