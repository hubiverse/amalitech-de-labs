"""
Author: Hubert Apana
Date: 2026-03-18

Data cleaning function for movie DataFrame.

This module provides a utility function for cleaning and preprocessing
a movie dataset represented as a pandas DataFrame. The function handles
operations such as dropping irrelevant columns, extracting relevant data
from JSON-like columns, type conversions, zero-value handling, value imputation,
string cleanup, and filtering to ensure the dataset is prepared for subsequent
analysis or modeling.

Functions:
    - clean_movie_df: Cleans and preprocesses the given DataFrame based
      on specified parameters and returns the cleaned DataFrame.
"""
import pandas as pd
import numpy as np

from dem02_tmdb_movie.utils.helpers import (
    safe_parse,
    extract_cast,
)

def clean_movie_df(df: pd.DataFrame, cols_to_drop=None, final_column_order=None) -> pd.DataFrame:
    """
    Cleans and preprocesses a DataFrame containing movie data.

    This function processes and cleans a DataFrame by handling missing or malformed
    values, extracting relevant information from nested structures, standardizing
    data types, and dropping irrelevant columns. It also computes additional
    features and ensures the resulting DataFrame meets specific constraints for
    further analysis.

    :param df: Input DataFrame containing raw movie data.
    :param cols_to_drop: List of column names to be dropped from the DataFrame.
                         If None, no columns are dropped.
    :param final_column_order: List specifying the desired column ordering in the
                               final DataFrame. If None, no reordering is done.
    :return: A cleaned and preprocessed DataFrame ready for analysis.
    """
    if final_column_order is None:
        final_column_order = []
    if cols_to_drop is None:
        cols_to_drop = []


    budget = pd.to_numeric(df['budget'], errors='coerce').replace(0, np.nan)
    revenue = pd.to_numeric(df['revenue'], errors='coerce').replace(0, np.nan)
    runtime = pd.to_numeric(df['runtime'], errors='coerce').replace(0, np.nan)

    vote_count = pd.to_numeric(df['vote_count'], errors='coerce').fillna(0)
    vote_average = pd.to_numeric(df['vote_average'], errors='coerce')
    vote_average = vote_average.mask(vote_count == 0, np.nan)

    runtime = runtime.fillna(runtime.median())


    df_cleaned = (
        df
        # Drop irrelevant columns
        .drop(columns=[c for c in cols_to_drop if c in df.columns])

        # Extract data from JSON-like columns
        .assign(
            # Parsed list and dictionaries
            belongs_to_collection=df['belongs_to_collection'].apply(safe_parse),
            genres=df['genres'].apply(safe_parse),
            spoken_languages=df['spoken_languages'].apply(safe_parse, key="english_name"),
            production_countries=df['production_countries'].apply(safe_parse),
            production_companies=df['production_companies'].apply(safe_parse),
            cast=df['cast'].apply(extract_cast),

            # Numeric
            id=pd.to_numeric(df['id'], errors='coerce'),
            budget=budget,
            revenue=revenue,
            runtime=runtime,
            popularity=pd.to_numeric(df['popularity'], errors='coerce'),
            vote_count=vote_count,
            vote_average=vote_average,

            # Dates
            release_date=pd.to_datetime(df['release_date'], errors='coerce'),

            # Derived
            budget_musd=budget / 1_000_000,
            revenue_musd=revenue / 1_000_000,

            # Strings
            overview=df['overview'].replace(['No Data', ''], np.nan),
            tagline=df['tagline'].replace(['No Data', ''], np.nan),
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
       df_cleaned = df_cleaned.reindex(columns=final_column_order).reset_index(drop=True)

    return  df_cleaned
