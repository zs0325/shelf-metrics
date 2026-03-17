import logging
from extract import extract_csv
from transform import (
    transform_authors,
    transform_books,
    transform_genres,
    transform_book_genres,
    transform_ratings
)
from load import (
    get_connection,
    load_authors,
    load_books,
    load_genres,
    load_book_genres,
    load_ratings
)

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    logging.info('Pipeline started')

    # Extract
    logging.info('--- Extract ---')
    books_raw     = extract_csv('data/books.csv')
    ratings_raw   = extract_csv('data/ratings.csv')
    tags_raw      = extract_csv('data/tags.csv')
    book_tags_raw = extract_csv('data/book_tags.csv')

    # Transform
    logging.info('--- Transform ---')
    authors_df     = transform_authors(books_raw)
    books_df       = transform_books(books_raw)
    genres_df      = transform_genres(tags_raw, book_tags_raw)
    book_genres_df = transform_book_genres(book_tags_raw, books_raw, books_df, genres_df)
    ratings_df = transform_ratings(ratings_raw, books_df)

    # Load
    logging.info('--- Load ---')
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


if __name__ == "__main__":
    run_pipeline()