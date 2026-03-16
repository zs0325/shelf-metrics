import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def transform_authors(books_df: pd.DataFrame) -> pd.DataFrame:
    # Returns Unique, non-null authors
    authors = (
        books_df['authors']
        .str.split(',')
        .explode()
        .str.strip()
        .dropna()
        .unique()
    )
    authors_df = pd.DataFrame({'name': authors})
    authors_df = authors_df[authors_df['name'] != '']
    logging.info(f'Transformed {len(authors_df)} unique authors')
    return authors_df


def transform_books(books_df: pd.DataFrame, authors_df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and rename only the columns the schema needs.
    Map author names to their assigned IDs from the authors table.
    """
    # Take the first listed author per book as the primary author
    books_df = books_df.copy()

    books_df['primary_author'] = (
        books_df['authors']
        .str.split(',')
        .str[0]
        .str.strip()
    )

    # Select and rename columns to match schema
    transformed = books_df[[
        'book_id',
        'primary_author',
        'title',
        'isbn',
        'original_publication_year',
        'average_rating',
        'ratings_count',
    ]].copy()

    transformed = transformed.rename(columns={
        'book_id': 'id',
        'original_publication_year': 'publication_year',
    })

    # Rrop rows where publication year is null, cast to int
    before = len(transformed)
    transformed = transformed.dropna(subset=['publication_year'])
    transformed['publication_year'] = transformed['publication_year'].astype(int)
    dropped = before - len(transformed)
    logging.info(f'Books: dropped {dropped} rows with null publication year')

    # Select rows only within the 1 - 5 range
    before = len(transformed)
    transformed = transformed[
        (transformed['average_rating'] >= 1) &
        (transformed['average_rating'] <= 5)
    ]
    dropped = before - len(transformed)
    logging.info(f'Books: dropped {dropped} rows with out-of-range average_rating')

    logging.info(f'Transformed {len(transformed)} books')
    return transformed


def transform_genres(tags_df: pd.DataFrame, book_tags_df: pd.DataFrame, min_count: int = 2000) -> pd.DataFrame:
     # Sum total count per tag across all books
    tag_totals = (
        book_tags_df
        .groupby('tag_id')['count']
        .sum()
        .reset_index()
        .rename(columns={'count': 'total_count'})
    )

    # Join with tag names
    tags_with_counts = tags_df.merge(tag_totals, on='tag_id', how='left')

    # Keep only most popular tags
    filtered = tags_with_counts[tags_with_counts['total_count'] >= min_count]

    genres_df = filtered[['tag_id', 'tag_name']].rename(columns={
        'tag_id': 'id',
        'tag_name': 'name'
    })

    logging.info(f'Genres: filtered from {len(tags_df)} tags to {len(genres_df)} genres')
    return genres_df

def transform_ratings(ratings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ratings are already clean (no nulls, 3 columns).
    Just validate the rating range and log anything dropped.
    """
    before = len(ratings_df)
    transformed = ratings_df[
        (ratings_df['rating'] >= 1) &
        (ratings_df['rating'] <= 5)
    ].copy()
    dropped = before - len(transformed)
    logging.info(f'Ratings: dropped {dropped} out-of-range rows from {before} total')
    logging.info(f'Transformed {len(transformed)} ratings')
    return transformed

def transform_book_genres(book_tags_df: pd.DataFrame, books_raw_df: pd.DataFrame, books_transformed_df: pd.DataFrame, genres_df: pd.DataFrame) -> pd.DataFrame:
    # Inner join book_tags and books on goodreads_book_id
    book_id_map = books_raw_df[['book_id', 'goodreads_book_id']].copy()
    merged = book_tags_df.merge(book_id_map, on='goodreads_book_id', how='inner')

    # Only keep tags from the genre tag filter
    valid_genre_ids = set(genres_df['id'])
    merged = merged[merged['tag_id'].isin(valid_genre_ids)]

    book_genres_df = merged[['book_id', 'tag_id']].rename(columns={
        'tag_id': 'genre_id'
    }).drop_duplicates()

    # Drop any book_ids that were filtered out during transform_books
    valid_book_ids = set(books_transformed_df['id'])
    before = len(book_genres_df)
    book_genres_df = book_genres_df[book_genres_df['book_id'].isin(valid_book_ids)]
    logging.info(f'Book-genres: dropped {before - len(book_genres_df)} rows referencing filtered books')

    logging.info(f'Transformed {len(book_genres_df)} book-genre associations')
    return book_genres_df

def transform_ratings(ratings_df: pd.DataFrame, books_df: pd.DataFrame) -> pd.DataFrame:
    # Only keep ratings for books that survived transform_books
    valid_book_ids = set(books_df['id'])
    before = len(ratings_df)
    ratings_df = ratings_df[ratings_df['book_id'].isin(valid_book_ids)]
    dropped = before - len(ratings_df)
    logging.info(f'Ratings: dropped {dropped} rows referencing filtered books')
    logging.info(f'Ratings: {len(ratings_df)} rows remaining')
    return ratings_df.copy()

if __name__ == "__main__":
    # For testing transform in isolation
    from extract import extract_csv

    books_raw    = extract_csv('data/books.csv')
    tags_raw     = extract_csv('data/tags.csv')
    book_tags_raw = extract_csv('data/book_tags.csv')
    ratings_raw   = extract_csv('data/ratings.csv')

    authors_df    = transform_authors(books_raw)
    books_df      = transform_books(books_raw, authors_df)
    genres_df     = transform_genres(tags_raw, book_tags_raw)
    book_genres_df = transform_book_genres(book_tags_raw, books_raw, books_df, genres_df)
    ratings_df = transform_ratings(ratings_raw, books_df)
    