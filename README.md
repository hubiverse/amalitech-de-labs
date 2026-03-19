# Amalitech Data Engineering Lab Submission
This repository contains the code and documentation for my submission to the Amalitech Data Engineering Labs. The 
project focuses on building a data pipeline that extracts, transforms, and loads (ETL/ELT) data from various sources 
for analysis.

## Set up
The project is built using the following:
- Python 3.14
- uv 0.10.11
The rest of the dependencies are listed in the `pyproject.toml` file.

## Installation
To install the project dependencies,
- Clone the repository
- Install `uv` by following the instructions [here](https://docs.astral.sh/uv/getting-started/installation)
- Run `uv sync` in the project directory to install the required packages
- Run jupyter notebook to open the project

## Environment Variables
The project uses the following environment variables:
- `TMDB_API_KEY`: The API key for TMDB.
- `TMDB_API_ACCESS_TOKEN`: The access token for TMDB API.
- `TMDB_API_BASE_URL`: The base URL for TMDB API.
TMBB API key can be obtained [here](https://www.themoviedb.org/settings/api). Create a new account if you don't have one.

**Create a `.env` file in the project root directory and add the following lines:**
```
TMDB_API_KEY=your_tmdb_api_key
TMDB_API_ACCESS_TOKEN=your_tmdb_api_access_token
TMDB_API_BASE_URL=https://api.themoviedb.org/3
```

## Usage
To run the project,
- Start jupyter server by running `uv run --with jupyter jupyter lab`
- Open the `<module>/main.ipynb` file in jupyter notebook
- Run the cells in the notebook

## Tests
To run the tests,
- Run `uv run pytest <module>/tests`

## Modules

### 1. DEM02: Python Programming 
#### TMDB Movie Data Analysis using Pandas and APIs
**Project Overview**

This project challenges you to build a movie data analysis pipeline using Python and Pandas. You will fetch 
movie-related data from an API, clean and transform the dataset, and implement key performance indicators (KPIs). 
This is not a group project, meaning you will design the workflow, structure the analysis, and implement the 
required calculations independently. 

**Project is located in the `dem02_tmdb_movie` directory.**
_Follow the jupyter notebook `main.ipynb` for analysis and easy code execution_
