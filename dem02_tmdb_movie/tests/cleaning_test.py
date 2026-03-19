"""

"""

import pandas as pd
from dem02_tmdb_movie.utils import clean_movie_df

def test_cleaning_pipeline_logic():
    budget = 356000000
    revenue = 2799439100
    raw_data = pd.DataFrame([{
        "id": 299534,
        "title": "Avengers: Endgame",
        "budget": budget,
        "revenue": revenue,
        "genres": [{"name": "Action"}, {"name": "Sci-Fi"}],
        "crew": [{"name": "Anthony Russo", "job": "Director"}, {"name": "Joe Russo", "job": "Director"}],
        "cast": [{"name": "Robert Downey Jr."}, {"name": "Chris Evans"}],
        "belongs_to_collection": {"name": "Avengers Collection"},
        "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
        "production_companies": [{"name": "Marvel Studios"}, {"name": "DC Comics"}],
        "spoken_languages": [{"english_name": "English", "iso_639_1": "en"}],
        "popularity": 10.0,
        "status": "Released",
        "tagline": "Never again will humanity be the same.",
        "release_date": "2019-04-24",
        "overview": "The Avengers and their allies must be willing to sacrifice all in an attempt to defeat the powerful Thanos before his blitz of devastation and ruin puts an end to the universe.",
        "vote_count": 10,
        "vote_average": 8.2,
        "runtime": 181
    }])

    cleaned_df = clean_movie_df(raw_data)

    budget_musd = budget / 1_000_000
    revenue_musd = revenue / 1_000_000

    assert cleaned_df.iloc[0]["belongs_to_collection"] == "Avengers Collection"
    assert cleaned_df.iloc[0]["production_countries"] == "United States of America"
    assert cleaned_df.iloc[0]["production_companies"] == "Marvel Studios|DC Comics"
    assert cleaned_df.iloc[0]["spoken_languages"] == "English"
    assert cleaned_df.iloc[0]["genres"] == "Action|Sci-Fi"
    assert cleaned_df.iloc[0]["budget_musd"] == budget_musd
    assert cleaned_df.iloc[0]["revenue_musd"] == revenue_musd
    assert type (cleaned_df.iloc[0]["release_date"]) == pd.Timestamp
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["id"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["budget"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["revenue"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["popularity"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["vote_count"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["runtime"])
    assert pd.api.types.is_numeric_dtype (cleaned_df.iloc[0]["vote_average"])
