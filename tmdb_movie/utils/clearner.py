import pandas as pd
import numpy as np

from tmdb_movie.utils.helpers import (
    safe_parse,
    extract_director,
    safe_len,
    extract_cast,
)

def clean_movie_df(df: pd.DataFrame, cols_to_drop=None, final_column_order=None) -> pd.DataFrame:
    if final_column_order is None:
        final_column_order = []
    if cols_to_drop is None:
        cols_to_drop = []

    df_cleaned = (
        df
        # Drop irrelevant columns
        .drop(columns=[c for c in cols_to_drop if c in df.columns])

        # Extract data from JSON-like columns
        .assign(
            belongs_to_collection = df['belongs_to_collection'].apply(safe_parse),
            genres = df['genres'].apply(safe_parse),
            spoken_languages = df['spoken_languages'].apply(safe_parse, key="english_name"),
            production_countries = df['production_countries'].apply(safe_parse),
            production_companies = df['production_companies'].apply(safe_parse),
            director = df['crew'].apply(extract_director),
            cast_size = df['cast'].apply(safe_len),
            crew_size = df['crew'].apply(safe_len),
            cast = df['cast'].apply(extract_cast),
        )

        # Convert types & Handle Zeros
        .assign(
            id = pd.to_numeric(df['id'], errors='coerce'),
            budget = pd.to_numeric(df['budget'], errors='coerce').replace(0, np.nan),
            revenue = pd.to_numeric(df['revenue'], errors='coerce').replace(0, np.nan),
            runtime = pd.to_numeric(df['runtime'], errors='coerce').replace(0, np.nan),
            popularity = pd.to_numeric(df['popularity'], errors='coerce'),
            release_date = pd.to_datetime(df['release_date'], errors='coerce'),
            vote_count = pd.to_numeric(df['vote_count'], errors='coerce').fillna(0),
        )


        # Impute values: Runtime with median, Vote Average with NaN if no votes
        .assign(
            runtime = df['runtime'].fillna(df['runtime'].median()),
            vote_average = pd.to_numeric(df['vote_average'], errors='coerce')
                                .mask(df['vote_count'] == 0, np.nan)
        )

        # Convert to Millions USD
        .assign(
            budget_musd = df['budget'] / 1_000_000,
            revenue_musd = df['revenue'] / 1_000_000
        )

        # Clean up strings & handle placeholders
        .assign(
            overview = df['overview'].replace(['No Data', ''], np.nan),
            tagline = df['tagline'].replace(['No Data', ''], np.nan)
        )

        # Filter: Only Released movies & Drop status
        .query("status == 'Released'")
        .drop(columns=['status', 'crew'], errors='ignore')

        # Drop duplicates and ensure columns aren't NaN
        .dropna(subset=['id', 'title'])
        .drop_duplicates(subset=['id'])

        # Keep only rows with at least 10 non-NaN values
        .dropna(thresh=10)
    )

    if len(final_column_order) > 0:
        # Reorder columns
       df_cleaned = df_cleaned .reindex(columns=final_column_order).reset_index(drop=True)

    return  df_cleaned
