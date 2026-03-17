import logging
import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def load_authors(authors_df: pd.DataFrame, conn) -> dict:
    # Insert authors first since their ids are used as foreign keys in book table
    cursor = conn.cursor()

    rows = [{'name': row['name']} for _, row in authors_df.iterrows()]

    psycopg2.extras.execute_batch(
        cursor,
        "INSERT INTO authors (name) VALUES (%(name)s) ON CONFLICT (name) DO NOTHING",
        rows
    )
    conn.commit()
    logging.info(f'Loaded {len(rows)} authors')

    # Fetch back all authors with their assigned IDs
    cursor.execute("SELECT id, name FROM authors")
    author_map = {name: id for id, name in cursor.fetchall()}
    cursor.close()
    return author_map


def load_books(books_df: pd.DataFrame, author_map: dict, conn):
    cursor = conn.cursor()

    books_df = books_df.copy()

    # Using map from load_authors looking up author name to get id
    books_df['author_id'] = books_df['primary_author'].map(author_map)

    # Drop any books where author couldn't be resolved
    before = len(books_df)
    books_df = books_df.dropna(subset=['author_id'])
    books_df['author_id'] = books_df['author_id'].astype(int)
    dropped = before - len(books_df)
    if dropped > 0:
        logging.warning(f'Books: dropped {dropped} rows where author_id could not be resolved')

    rows = books_df[[
        'id', 'author_id', 'title', 'isbn',
        'publication_year', 'average_rating', 'ratings_count'
    ]].to_dict('records')

    psycopg2.extras.execute_batch(
        cursor,
        """
        INSERT INTO books (id, author_id, title, isbn, publication_year, average_rating, ratings_count)
        VALUES (%(id)s, %(author_id)s, %(title)s, %(isbn)s, %(publication_year)s, %(average_rating)s, %(ratings_count)s)
        ON CONFLICT (id) DO NOTHING
        """,
        rows
    )
    conn.commit()
    logging.info(f'Loaded {len(rows)} books')
    cursor.close()


def load_genres(genres_df: pd.DataFrame, conn):
    cursor = conn.cursor()

    rows = genres_df[['id', 'name']].to_dict('records')

    psycopg2.extras.execute_batch(
        cursor,
        "INSERT INTO genres (id, name) VALUES (%(id)s, %(name)s) ON CONFLICT (name) DO NOTHING",
        rows
    )
    conn.commit()
    logging.info(f'Loaded {len(rows)} genres')
    cursor.close()


def load_book_genres(book_genres_df: pd.DataFrame, conn):
    cursor = conn.cursor()

    rows = book_genres_df.to_dict('records')

    psycopg2.extras.execute_batch(
        cursor,
        """
        INSERT INTO book_genres (book_id, genre_id)
        VALUES (%(book_id)s, %(genre_id)s)
        ON CONFLICT DO NOTHING
        """,
        rows
    )
    conn.commit()
    logging.info(f'Loaded {len(rows)} book-genre associations')
    cursor.close()


def load_ratings(ratings_df: pd.DataFrame, conn):
    cursor = conn.cursor()

    rows = ratings_df.to_dict('records')

    psycopg2.extras.execute_batch(
        cursor,
        """
        INSERT INTO ratings (book_id, user_id, rating)
        VALUES (%(book_id)s, %(user_id)s, %(rating)s)
        ON CONFLICT DO NOTHING
        """,
        rows,
        page_size=1000
    )
    conn.commit()
    logging.info(f'Loaded {len(rows)} ratings')
    cursor.close()


if __name__ == "__main__":
    from extract import extract_csv
    from transform import (
        transform_authors, transform_books, transform_genres,
        transform_book_genres, transform_ratings
    )

    books_raw     = extract_csv('data/books.csv')
    ratings_raw   = extract_csv('data/ratings.csv')
    tags_raw      = extract_csv('data/tags.csv')
    book_tags_raw = extract_csv('data/book_tags.csv')

    authors_df     = transform_authors(books_raw)
    books_df       = transform_books(books_raw)
    genres_df      = transform_genres(tags_raw, book_tags_raw)
    book_genres_df = transform_book_genres(book_tags_raw, books_raw, books_df, genres_df)
    ratings_df = transform_ratings(ratings_raw, books_df)

    conn = get_connection()

    try:
        author_map = load_authors(authors_df, conn)
        load_books(books_df, author_map, conn)
        load_genres(genres_df, conn)
        load_book_genres(book_genres_df, conn)
        load_ratings(ratings_df, conn)
        logging.info('Pipeline complete')
    except Exception as e:
        conn.rollback()
        logging.error(f'Pipeline failed: {e}')
        raise
    finally:
        conn.close()