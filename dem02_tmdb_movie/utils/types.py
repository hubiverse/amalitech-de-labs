"""
Author: Hubert Apana
Date: 2026-03-18

Pydantic models for TMDB movie data. These models are used to parse and validate the data returned by the
TMDB API when fetching movie details, including genres, production companies, production countries, spoken languages,
 cast, and crew information.
"""

from pydantic import BaseModel, ConfigDict, Field

class MovieGenre(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int = 0
    name: str | None = None

class MovieProductionCompany(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int | None = None
    name: str | None = None
    logo_path: str | None = None
    origin_country: str | None = None

class MovieProductionCountry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    iso_3166_1: str | None = None
    name: str | None = None

class MovieSpokenLanguage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    iso_639_1: str | None = None
    name: str | None = None
    english_name: str | None = None

class MovieCast(BaseModel):
    model_config = ConfigDict(extra="ignore")
    adult: bool | None = None
    gender: int | None = None
    id: int | None = None
    known_for_department: str | None = None
    name: str | None = None
    original_name: str | None = None
    popularity: float | None = None
    profile_path: str | None = None
    cast_id: int | None = None
    character: str | None = None
    credit_id: str | None = None
    order: int | None = None

class MovieCrew(BaseModel):
    model_config = ConfigDict(extra="ignore")
    adult: bool | None = None
    gender: int | None = None
    id: int | None = None
    known_for_department: str | None = None
    name: str | None = None
    original_name: str | None = None
    popularity: float | None = None
    profile_path: str | None = None
    credit_id: str | None = None
    department: str | None = None
    job: str | None = None

class Movie(BaseModel):
    model_config = ConfigDict(extra="ignore")

    adult: bool | None = None
    backdrop_path: str | None = None
    belongs_to_collection: dict | None = None
    budget: float | None = None
    genres: list[MovieGenre] = Field(default_factory=list)
    homepage: str | None = None
    id: int
    imdb_id: str | None = None
    original_language: str | None = None
    original_title: str | None = None
    overview: str | None = None
    popularity: float | None = None
    poster_path: str | None = None
    production_companies: list[MovieProductionCompany] = Field(default_factory=list)
    production_countries: list[MovieProductionCountry] = Field(default_factory=list)
    release_date: str | None = None
    revenue: float | None = None
    runtime: float | None = None
    spoken_languages: list[MovieSpokenLanguage] = Field(default_factory=list)
    status: str | None = None
    tagline: str | None = None
    title: str | None = None
    video: bool | None = None
    vote_average: float | None = None
    vote_count: int | None = None

    cast: list[MovieCast] = Field(default_factory=list)
    crew: list[MovieCrew] = Field(default_factory=list)
