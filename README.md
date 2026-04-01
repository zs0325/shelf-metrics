# ShelfMetrics

A Python ETL pipeline that ingests the Goodreads public dataset, cleans and loads it into a structured PostgreSQL database, and demonstrates progressive SQL analysis across 7 documented challenge files.

The project is split into two layers: an **engineering layer** (Python ETL) and an **analytical layer** (SQL challenges). Together they demonstrate Python data processing, relational database design, query optimisation, and table partitioning on a dataset of nearly 10,000 books and 4.6 million user ratings.

---

## Tech Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Language | Python 3.x | Industry standard for ETL and data processing |
| Data Processing | pandas | Efficient DataFrame manipulation for cleaning and reshaping |
| Database Connector | psycopg2 | Lightweight, direct PostgreSQL adapter for Python |
| Database | PostgreSQL | Production-grade relational database with native partitioning support |
| Dataset | Goodreads Books (Kaggle) | Large, realistic, multi-entity public dataset |

---

## Dataset

The dataset is sourced from Kaggle and is not committed to this repository. Raw CSV files must be downloaded and placed in the `data/` directory before running the pipeline.

**Download:** Search [Goodreads Books dataset on Kaggle](https://www.kaggle.com) and download the version containing the following files:

| File | Description | Rows |
|------|-------------|------|
| `books.csv` | Book metadata — title, author, ISBN, ratings | 10,000 |
| `ratings.csv` | Individual user ratings (1–5) | 5,976,479 |
| `tags.csv` | Tag/shelf name lookup | 34,252 |
| `book_tags.csv` | Book to tag associations with frequency counts | 999,912 |

Place all four files in the `data/` folder. They are excluded from version control via `.gitignore`.

---

## Database Schema

The schema normalises the flat CSV structure into five related tables. Table creation scripts live in `schema/schema.sql`.

```
authors
  id    SERIAL PRIMARY KEY
  name  VARCHAR(255) UNIQUE NOT NULL

books
  id                SERIAL PRIMARY KEY
  author_id         INT → authors(id)
  title             VARCHAR(500)
  isbn              VARCHAR(20)
  publication_year  INT
  average_rating    DECIMAL(3,2)
  ratings_count     INT

genres
  id    SERIAL PRIMARY KEY
  name  VARCHAR(100) UNIQUE NOT NULL

book_genres  (junction)
  book_id   INT → books(id)
  genre_id  INT → genres(id)
  PRIMARY KEY (book_id, genre_id)

ratings  (partitioned)
  id       SERIAL
  book_id  INT → books(id)
  user_id  INT
  rating   SMALLINT (1–5)
  PRIMARY KEY (id, rating)
  PARTITION BY RANGE (rating)
    ratings_low   → rating 1–2
    ratings_mid   → rating 3
    ratings_high  → rating 4–5
```

### Partitioning Design Decision

The `ratings` table is partitioned by range on the `rating` column. With 4.6 million rows, partitioning allows PostgreSQL to prune irrelevant partitions when queries filter on rating value — scanning only `ratings_low`, `ratings_mid` or `ratings_high` rather than the full table.

This is demonstrated with `EXPLAIN ANALYZE` output in `analyses/07_partitioning_analysis.sql`.

---

## ETL Pipeline

The pipeline follows a strict Extract → Transform → Load pattern. Each stage is isolated in its own module.

```
etl/
├── extract.py      Load raw CSVs into DataFrames with shape and null logging
├── transform.py    Clean, reshape, validate, and map data to schema structure
├── load.py         Batch insert transformed DataFrames into PostgreSQL
└── pipeline.py     Orchestrate the full E → T → L flow
```

### Transform Summary

| Table | Input Rows | Loaded Rows | Notes |
|-------|-----------|-------------|-------|
| books | 10,000 | 9,979 | 21 dropped — null publication year |
| genres | 34,252 tags | 19 | Curated list of meaningful genre categories |
| book_genres | 999,912 | 50,283 | Filtered to curated genres and valid book IDs only |
| ratings | 5,976,479 | 4,600,763 | Filtered to books that survived transform step |

---

## Local Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 16+
- A Kaggle account to download the dataset

### 1. Clone the repository

```bash
git clone https://github.com/zs0325/shelf-metrics.git
cd shelf-metrics
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download the dataset

Download the Goodreads Books dataset from Kaggle and place the four CSV files in the `data/` folder:

```
shelf-metrics/
└── data/
    ├── books.csv
    ├── ratings.csv
    ├── tags.csv
    └── book_tags.csv
```

### 5. Create the database

```bash
psql -U postgres -c "CREATE DATABASE goodbooks;"
```

### 6. Run the schema

```bash
psql -U postgres -d goodbooks -f schema/schema.sql
```

### 7. Configure environment variables

Copy the example file and fill in your database credentials:

```bash
cp .env.example .env
```

### 8. Run the pipeline

```bash
python3 etl/pipeline.py
```

## SQL Analyses

Seven SQL challenge files live in the `analyses/` directory. 

| File | Concept | Description |
|------|---------|-------------|
| `01_basic_aggregations.sql` | Aggregations | Books per genre, average rating per genre, publications per decade |
| `02_joins_and_filtering.sql` | JOINs | Multi-table joins filtering by rating threshold and publication year |
| `03_top_per_genre.sql` | ROW_NUMBER() | Top 5 highest rated books per genre with minimum ratings threshold |
| `04_window_functions.sql` | RANK(), DENSE_RANK() | Authors ranked within genre, rolling average rating by publication year |
| `05_rating_distribution.sql` | CASE, percentages | Rating distribution per genre — identifying polarised vs consistent genres |
| `06_author_consistency.sql` | STDDEV, subqueries | Authors with lowest rating variance across their books |
| `07_partitioning_analysis.sql` | Partition pruning | Demonstrating partition pruning benefit with EXPLAIN ANALYZE output |

### Running the analyses

```bash
psql -h localhost -d goodbooks -U postgres -f analyses/01_basic_aggregations.sql
```
---


### Power BI Dashboard views
<img width="1317" height="745" alt="Screenshot 2026-04-01 180114" src="https://github.com/user-attachments/assets/552a90f7-224d-4035-90f8-8d5abec71b0e" />
<img width="1317" height="737" alt="Screenshot 2026-04-01 180129" src="https://github.com/user-attachments/assets/c9488554-beb2-435a-b1df-de109d9a7369" />
<img width="1323" height="743" alt="Screenshot 2026-04-01 181027" src="https://github.com/user-attachments/assets/9a649c77-39dd-4571-a8bd-f1fd416ce654" />

## Data Quality Notes
Several data quality issues were encountered and handled during the transform step:![Uploading Screenshot 2026-04-01 180129.png…]()

 
- **Authors column** contains comma-separated multiple authors — only the primary author is extracted per book. Co-authored books are attributed to the first listed author.
- **21 books** had null `original_publication_year` and were dropped — a year is required for decade-based analyses.
- **Genre tags** in the raw dataset are user-defined Goodreads shelves, not a controlled vocabulary. The full 34,252 tags include reading lists, challenge shelves, and personal categories. A curated list of 19 standard genre names was used instead.
