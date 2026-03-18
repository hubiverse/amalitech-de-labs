"""
Unit tests for validating the fetch_movie_by_id function's behavior under different scenarios.

These tests simulate various conditions such as successful responses, server errors,
non-retriable errors, and retry logic. The package leverages the `respx` mock
library to intercept HTTP requests, allowing controlled responses for testing
purposes. This suite is designed to ensure robustness and correctness
in the tmdb_movie API integration.

Fixtures:
    mock_settings: Provides mock configuration options such as API keys
        and base URL to simulate integration with the TMDB API.
    client: An HTTP client instance with a predefined timeout used for
        making API requests within the tests.

Tests:
    1. test_fetch_movie_happy_path: Validates a successful movie fetch scenario.
    2. test_fetch_movie_hard_error_no_retry: Ensures that no retries occur for
       non-retriable errors like HTTP 404.
    3. test_fetch_movie_soft_error_retry_success: Tests the retry mechanism when
       encountering transient server errors.
    4. test_max_retries_logic: Confirms that the retry logic adheres to the
       provided `max_retries` value.
"""
import httpx
import pytest
import respx
from httpx import Response
from dem02_tmdb_movie.utils import fetch_movie_by_id, movie_url
from config import Settings


@pytest.fixture
def mock_settings():
    return Settings(
        tmdb_api_base_url="https://api.themoviedb.org/3",
        tmdb_api_access_token="fake_token",
        tmdb_api_key="fake_key"
    )

@pytest.fixture
def client():
    return httpx.Client(timeout=10.0)


@respx.mock
def test_fetch_movie_happy_path(mock_settings, client):
    """
    Mocks the fetch movie endpoint to test successful response.

    The test simulates a happy path scenario where the external API successfully
    returns movie details for a specific movie ID. The mocked API response is
    utilized instead of making a real HTTP request. The test asserts the output
    for correctness by comparing the result to expected movie details.
    """
    movie_id = 123
    # Setup mock
    respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id=movie_id)).mock(
        return_value=Response(200, json={"id": movie_id, "title": "Test Movie", "status": "Released"})
    )

    movie = fetch_movie_by_id(client, mock_settings, movie_id)

    assert movie is not None
    assert movie.title == "Test Movie"
    assert movie.id == movie_id


@respx.mock
def test_fetch_movie_hard_error_no_retry(mock_settings, client):
    """
    Mocks a movie-fetching API endpoint to simulate hard errors and verifies that no retries
    are performed for a 404 error response.

    The function tests the behavior of fetching a movie by its ID, ensuring that a 404 error
    is treated as non-retriable, and validates that the API is only called once.
    """
    movie_id = 999
    # Mock a 404
    route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(404)
    )

    movie = fetch_movie_by_id(client, mock_settings, movie_id, max_retries=3)

    assert movie is None
    # Verify it only tried ONCE (no retries for 404)
    assert route.called
    assert route.call_count == 1


@respx.mock
def test_fetch_movie_soft_error_retry_success(mock_settings, client):
    """
    Test the fetch_movie_by_id function to ensure correct retry behavior when the server
    initially returns a 500 error and subsequently succeeds with a 200 response.

    This test mimics the scenario where the server encounters a transient error and recovers,
    validating that the fetch_movie_by_id function handles retries correctly and ultimately
    retrieves the expected movie details.
    """
    movie_id = 123
    # Mock a 500 error followed by a 200 success
    route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).side_effect = [
        Response(500),
        Response(200, json={"id": movie_id, "title": "Recovered Movie", "status": "Released"})
    ]

    movie = fetch_movie_by_id(client, mock_settings, movie_id, max_retries=2, wait_factor=0.1)

    assert movie.title == "Recovered Movie"
    assert movie.id == movie_id

    # Verify it was called twice
    assert len(respx.calls) == 2


@pytest.mark.parametrize("max_retries, expected_calls", [(0, 1), (2, 3)])
@respx.mock
def test_max_retries_logic(mock_settings, client, max_retries, expected_calls):
    """
    Tests the retry logic for HTTP requests made to an external API. This test ensures that the
    correct number of retries is performed based on the specified `max_retries` value.

    The test uses the `respx` library to mock an HTTP route that responds with a persistent 500
    error, simulating a scenario where the server is unavailable. It asserts that the number
    of actual calls made aligns with the calculated `expected_calls`.

    :param mock_settings: Configuration or settings used in the test environment, such as API keys
        or endpoint configurations.
    :param client: An HTTP client instance that performs the requests to the API.
    :param max_retries: The maximum number of retry attempts allowed when an HTTP request fails.
        This parameter influences the total number of actual HTTP calls.
    :param expected_calls: The expected total number of calls made to the mocked endpoint, which
        is calculated based on the `max_retries` value. A higher `max_retries` leads to more calls.
    :return: None
    """
    movie_id = 123
    # Mock persistent 500 error
    route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(500)
    )

    movie = fetch_movie_by_id(client, mock_settings, movie_id, max_retries=max_retries, wait_factor=0.01)

    assert movie is None
    assert route.call_count == expected_calls