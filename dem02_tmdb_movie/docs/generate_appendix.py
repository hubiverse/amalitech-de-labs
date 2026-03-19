from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config import get_settings
from dem02_tmdb_movie.utils import (
     get_movies_dataframe_from_ids,
    clean_movie_df
)

# Download movies
MOVIE_IDS = [
    0,
    299534,
    19995,
    140607,
    299536,
    597,
    135397,
    420818,
    24428,
    168259,
    99861,
    284054,
    12445,
    181808,
    330457,
    351286,
    109445,
    321612,
    260513,
]

df, failed = get_movies_dataframe_from_ids(settings=get_settings(), movie_ids=MOVIE_IDS, max_retries=0)

if failed:
    print(f"\nThe following IDs could not be fetched: {failed}")

# Define the columns to drop
cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

# Define the final column order as per instructions
final_column_order = [
    'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
    'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
]

# Cleaning pipeline using chaining
df_cleaned  = clean_movie_df(df, cols_to_drop=cols_to_drop, final_column_order=final_column_order)

# Add Profit and ROI columns to our cleaned dataframe
df_final = df_cleaned.assign(
    profit_musd =  df_cleaned['revenue_musd'] - df_cleaned['budget_musd'],
    roi = df_cleaned['revenue_musd'] / df_cleaned['budget_musd']
)

## help functions
def rank_movies(movie_df, column, n=5, ascending=False, min_votes=0, min_budget=0):
    """
    Ranks movies based on a specific column with optional quality filters.
    """
    return (
        movie_df
        .query(f"vote_count >= {min_votes} and budget_musd >= {min_budget}")
        .sort_values(by=column, ascending=ascending)
        [['title', column, 'vote_count', 'budget_musd', 'revenue_musd']]
        .head(n)
    )

def df_to_typst(df, caption="", columns_spec=None, label=""):
    if not columns_spec:
        columns_spec = f"({', '.join(['1fr'] * len(df.columns))})"

    typst_lines = []
    typst_lines.append(f"#figure(")
    typst_lines.append(f"  table(")
    typst_lines.append(f"    columns: {columns_spec},")
    typst_lines.append(f"    inset: 10pt,")
    # Logic: If column (x) is 0, align left. Otherwise, center.
    typst_lines.append(f"    align: (x, y) => if x == 0 {{ left + horizon }} else {{ center + horizon }},")
    typst_lines.append(f"    fill: (x, y) => if y == 0 {{ gray.lighten(60%) }},")

    headers = ", ".join([f"[* {col.replace('_', ' ').title()} *]" for col in df.columns])
    typst_lines.append(f"    {headers},")

    for _, row in df.iterrows():
        row_vals = []
        for val in row:
            if pd.isna(val): row_vals.append("[---]")
            elif isinstance(val, (int, float)): row_vals.append(f"[{val:,.2f}]")
            else: row_vals.append(f"[{str(val)}]")
        typst_lines.append(f"    {', '.join(row_vals)},")

    typst_lines.append("  ),")
    if caption: typst_lines.append(f"  caption: [{caption}],")
    if label: typst_lines.append(f") <{label}>")
    else: typst_lines.append(")")

    return "\n".join(typst_lines)


# ============= KPIs ===========================
# 1. Highest Revenue
top_revenue = rank_movies(df_final, 'revenue_musd')

# 2. Highest Budget
top_budget = rank_movies(df_final, 'budget_musd')

# 3. Highest Profit
top_profit = rank_movies(df_final, 'profit_musd')

# 4. Lowest Profit (Worst Financial Performance)
worst_profit = rank_movies(df_final, 'profit_musd', ascending=True)

# 5. Highest ROI (Only movies with Budget >= 10M)
top_roi = rank_movies(df_final, 'roi', min_budget=10)

# 6. Lowest ROI (Only movies with Budget >= 10M)
worst_roi = rank_movies(df_final, 'roi', min_budget=10, ascending=True)

# 7. Most Voted
most_voted = rank_movies(df_final, 'vote_count')

# 8. Highest Rated (Only movies with >= 10 votes)
top_rated = rank_movies(df_final, 'vote_average', min_votes=10)

# 9. Lowest Rated (Only movies with >= 10 votes)
worst_rated = rank_movies(df_final, 'vote_average', min_votes=10, ascending=True)

# 10. Most Popular
most_popular = rank_movies(df_final, 'popularity')

# Search 1: Find the best-rated Science Fiction Action movies starring Bruce Willis (sorted by Rating - highest to lowest).
search_bruce = (
    df_final
    .loc[
        df_final['cast'].str.contains("Bruce Willis", case=False, na=False) &
        df_final['genres'].str.contains("Action", case=False, na=False) &
        df_final['genres'].str.contains("Science Fiction", case=False, na=False)
    ]
    .sort_values(by='vote_average', ascending=False)
)

# Search 2: Find movies starring Uma Thurman, directed by Quentin Tarantino (sorted by runtime - shortest to longest).
search_quentin = (
    df_final
    .loc[
        df_final['director'].str.contains("Quentin Tarantino", case=False, na=False) &
        df_final['cast'].str.contains("Uma Thurman", case=False, na=False)
    ]
    .sort_values(by='runtime', ascending=True)
)

# Compare movie franchises (belongs_to_collection) vs. standalone movies
franchise_comparison = (
    df_final
    .assign(status =  np.where(df_final['belongs_to_collection'].isna(), "Standalone", "Franchise"))
    .groupby('status')
    .agg(
        mean_revenue = ('revenue_musd', 'mean'),
        median_roi = ('roi', 'median'),
        mean_budget = ('budget_musd', 'mean'),
        mean_popularity = ('popularity', 'mean'),
        mean_rating = ('vote_average', 'mean')
    )
)

#  Find the Most Successful Movie Franchises based on:
top_franchises = (
    df_final
    .dropna(subset=['belongs_to_collection'])
    .groupby('belongs_to_collection')
    .agg(
        movie_count = ('id', 'count'),
        total_budget = ('budget_musd', 'sum'),
        mean_budget = ('budget_musd', 'mean'),
        total_revenue = ('revenue_musd', 'sum'),
        mean_revenue = ('revenue_musd', 'mean'),
        mean_rating = ('vote_average', 'mean')
    )
    .sort_values(by='movie_count', ascending=False)
    .head(5)
)

# Find the Most Successful Directors based on:
top_directors = (
    df_final
    .dropna(subset=['director'])
    .assign(director = df_final['director'].str.split('|'))
    .explode('director')
    .groupby('director')
    .agg(
        movie_count = ('id', 'count'),
        total_revenue = ('revenue_musd', 'sum'),
        avg_rating = ('vote_average', 'mean')
    )
    .sort_values(by='total_revenue', ascending=False)
    .head(5)
)


### ============ Plots ===================
# Plots directory
base_dir = Path(__file__).parent
plots_dir = base_dir / 'plots'
plots_dir.mkdir(exist_ok=True)
appendix_file = base_dir / 'appendix.typ'

# Revenue vs. Budget Trends
plt.figure(figsize=(10, 6))
sns.set_style("whitegrid")

# Scatter plot
sns.scatterplot(
    data=df_final,
    x='budget_musd',
    y='revenue_musd',
    size='popularity',
    hue='roi',
    palette='viridis',
    sizes=(50, 500)
)

# Add a diagonal line for breakeven (Revenue = Budget)
max_val = max(df_final['revenue_musd'].max(), df_final['budget_musd'].max())
plt.plot([0, max_val/5], [0, max_val/5], color='red', linestyle='--', label='Breakeven Line (1x)')

plt.title('Movie Success: Revenue vs. Budget (Millions USB)', fontsize=15)
plt.xlabel('Budget (Million USD)')
plt.ylabel('Revenue (Million USD)')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2)
plt.savefig(plots_dir / 'revenue_vs_budget.png')

# ROI Distribution by Genre
genre_roi = (
    df_final[['id', 'genres', 'revenue_musd', 'roi', 'budget_musd']]
    .dropna(subset=['genres'])
    .assign(genres = df_final['genres'].str.split('|'))
    .explode('genres')
    .groupby('genres')["roi"]
    .median()
    .sort_values(ascending=False)
)

plt.figure(figsize=(12, 6))
sns.barplot(
    x=genre_roi.values,
    y=genre_roi.index,
    hue=genre_roi.index,
    palette='magma',
    legend=False
)
plt.title('Median ROI Multiple by Genre', fontsize=15)
plt.xlabel('ROI (Revenue / Budget)')
plt.ylabel('Genre')
plt.axvline(1, color='red', linestyle='--', label='Breakeven (1x)')
plt.savefig(plots_dir / 'roi_by_genre.png')

# Popularity vs. Rating
plt.figure(figsize=(10, 6))
sns.regplot(data=df_final, x='vote_average', y='popularity',
            scatter_kws={'s':df_final['vote_count']/100, 'alpha':0.5}, # Size based on vote count
            line_kws={'color':'red'})

plt.title('Popularity vs. User Rating', fontsize=15)
plt.xlabel('Average Rating (out of 10)')
plt.ylabel('Popularity Score')
plt.savefig(plots_dir / 'popularity_vs_rating.png')

# Yearly Trends in Box Office Performance
# Extract Year and group
yearly_trends = (
    df_final
    .assign(year = df_final['release_date'].dt.year)
    .groupby('year')
    .agg({'revenue_musd': 'sum', 'budget_musd': 'sum'})
)

plt.figure(figsize=(12, 6))
plt.plot(yearly_trends.index, yearly_trends['revenue_musd'], marker='o', label='Total Revenue', linewidth=2)
plt.plot(yearly_trends.index, yearly_trends['budget_musd'], marker='s', label='Total Budget', linestyle='--')

plt.fill_between(
    yearly_trends.index,
     yearly_trends['budget_musd'],
     yearly_trends['revenue_musd'],
     color='green',
     alpha=0.1,
     label='Total Profit Zone'
)

plt.title('Yearly Box Office Performance', fontsize=15)
plt.xlabel('Year')
plt.ylabel('Millions USD')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig(plots_dir / 'yearly_box_office.png')

# Comparison of Franchise vs. Standalone Success
df_viz = (
    df_final
    .assign(type = np.where(df_final['belongs_to_collection'].isna(), "Standalone", "Franchise"))
)

fig, ax = plt.subplots(2, 3, figsize=(14, 6))

comp_plots = [
    {
        "y": "revenue_musd",
        "y_label": "Revenue (Millions USD)",
        "title": "Mean Revenue: Franchise vs Standalone",
        "palette": "Blues",
        "estimator": np.mean,
        "position": (0, 0)
    },
    {
        "y": "roi",
        "y_label": "Median ROI (Revenue / Budget)",
        "title": "Median ROI: Franchise vs Standalone",
        "palette": "Oranges",
        "estimator": np.median,
        "position": (1, 0)
    },
    {
        "y": "budget_musd",
        "y_label": "Mean Budget (Millions USD)",
        "title": "Mean Budget: Franchise vs Standalone",
        "palette": "Greens",
        "estimator": np.mean,
        "position": (0, 1)
    },
    {
        "y": "popularity",
        "y_label": "Mean Popularity",
        "title": "Mean Popularity: Franchise vs Standalone",
        "palette": "Purples",
        "estimator": np.mean,
        "position": (1, 1)
    },
    {
        "y": "vote_average",
        "y_label": "Mean Rating (out of 10)",
        "title": "Mean Rating: Franchise vs Standalone",
        "palette": "Reds",
        "estimator": np.mean,
        "position": (0, 2)
    },
    {
        "y": "vote_count",
        "y_label": "Mean Vote Count",
        "title": "Mean Vote Count: Franchise vs Standalone",
        "palette": "Greys",
        "estimator": np.mean,
        "position": (1, 2)
    }

]

for comp_plot in comp_plots:
    sns.barplot(
        data=df_viz,
        x='type',
        y=comp_plot['y'],
        estimator=comp_plot['estimator'],
        hue='type',
        ax=ax[comp_plot['position'][0], comp_plot['position'][1]],
        palette=comp_plot['palette'],
        legend=False
    )

    ax[comp_plot['position'][0], comp_plot['position'][1]].set_title(comp_plot['title'])
    ax[comp_plot['position'][0], comp_plot['position'][1]].set_ylabel(comp_plot['y_label'])


plt.tight_layout()
plt.savefig(plots_dir / 'franchise_vs_standalone.png')

appendix_content = [
    "= Appendix <appendix>",
    f"== Appendix A: Highest Revenue <appendix-highest-revenue>\n",
    f'{
        df_to_typst(
            top_revenue, "Top 5 Highest Revenue Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_revenue"
        )
    }',
    f" == Appendix B: Highest Budget <appendix-highest-budget>\n",
    f'{
        df_to_typst(
            top_budget, "Top 5 Highest Budget Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_budget"
        )
    }',
    f"== Appendix C: Highest Profit <appendix-highest-profit>\n"
    f'{
        df_to_typst(
            top_profit, "Top 5 Highest Profit Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_profit"
        )
    }',
    f"== Appendix D: Lowest Profit <appendix-lowest-profit>\n",
    f'{
        df_to_typst(
            worst_profit, "Top 5 Lowest Profit Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_profit"
        )
    }',
    f"== Appendix E: Highest ROI <appendix-highest-roi>\n",
    f'{
        df_to_typst(
            top_roi, "Top 5 Highest ROI Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_roi"
        )
    }',
    f"== Appendix F: Lowest ROI <appendix-lowest-roi>\n",
    f'{
        df_to_typst(
            worst_roi, "Top 5 Lowest ROI Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_roi"
        )
    }',
    f"== Appendix G: Most Voted <appendix-most-voted>\n",
    f'{
        df_to_typst(
            most_voted, "Top 5 Most Voted Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:most_voted"
        )
    }',
    f"== Appendix H: Highest Rated <appendix-highest-rated>\n",
    f'{
        df_to_typst(
            top_rated, "Top 5 Highest Rated Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_rated"
        )
    }',
    f"== Appendix I: Lowest Rated <appendix-lowest-rated>\n",
    f'{
        df_to_typst(
            worst_rated, "Top 5 Lowest Rated Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_rated"
        )
    }',
    f"== Appendix J: Most Popular <appendix-most-popular>\n",
    f'{
        df_to_typst(
            most_popular, "Top 5 Most Popular Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:most_popular"
        )
    }',
    f"== Appendix K: Best-rated Science Fiction Action Movies starring Bruce Willis <appendix-best-rated-bruce-willis>\n",
    f'{
        df_to_typst(
            rank_movies(search_bruce, column='vote_average'),
            "Best-rated Science Fiction Action Movies starring Bruce Willis", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:search_bruce"
        )
    }',
    f"== Appendix L: Shortest Movies starring Uma Thurman directed by Quentin Tarantino <appendix-shortest-tarantino>\n",
    f'{
        df_to_typst(
            rank_movies(search_quentin, column='runtime', ascending=True),
            "Shortest Movies starring Uma Thurman directed by Quentin Tarantino", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:search_quentin"
        )
    }',
    f"== Appendix M: Franchise vs. Standalone <appendix-franchise-standalone>\n",
    f'{
        df_to_typst(
            franchise_comparison, "Franchise vs. Standalone Movie Comparison", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:franchise_comparison"
        )
    }',
    f"== Appendix N: Most Successful Franchises <appendix-most-successful-franchises>\n",
    f'{
        df_to_typst(
            top_franchises, "Top 5 Most Successful Movie Franchises", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_franchises"
        )
    }',
    f"== Appendix O: Most Successful Directors <appendix-most-successful-directors>\n",
    f'{
        df_to_typst(
            top_directors, "Top 5 Most Successful Directors", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_directors"
        )
    }'
]

visual_analysis_content = f"""
== Appendix P: Visual Analysis <appendix-visuals>

#figure(
  image("plots/revenue_vs_budget.png", width: 90%),
  caption: [Revenue vs. Budget Trends. The red dashed line represents the 1.0x Multiple (Breakeven). Size indicates movie popularity.],
) <fig-rev-bud>

#v(1cm)

#grid(
  columns: (1fr, 1fr),
  gutter: 15pt,
  [
    #figure(
      image("plots/roi_by_genre.png", width: 100%),
      caption: [Median ROI Multiple by Genre. Note the efficiency of Sci-Fi and Adventure genres.],
    ) <fig-genre-roi>
  ],
  [
    #figure(
      image("plots/popularity_vs_rating.png", width: 100%),
      caption: [Popularity Score vs. User Rating. The red regression line indicates the trend of audience reception.],
    ) <fig-pop-rating>
  ]
)

#v(1cm)

#figure(
  image("plots/yearly_box_office.png", width: 90%),
  caption: [Yearly Box Office Performance. The shaded green area represents the aggregate profit zone across the sample set.],
) <fig-yearly-trends>

#v(1cm)

#figure(
  image("plots/franchise_vs_standalone.png", width: 100%),
  caption: [Comprehensive Comparison: Franchise vs. Standalone Performance across six key metrics (Revenue, ROI, Budget, Popularity, Rating, and Vote Count).],
) <fig-franchise-comp>
"""

appendix_content.append(visual_analysis_content)

# Final save
with open(appendix_file, "w", encoding="utf-8") as f:
    f.write("\n\n".join(appendix_content))

print(f"Successfully generated {appendix_file}")
