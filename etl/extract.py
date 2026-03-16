import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def extract_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
    logging.info(f'Extracted {len(df)} rows and {len(df.columns)} columns from {filepath}')
    logging.info(f'Columns: {list(df.columns)}')
    logging.info(f'Null counts:\n{df.isnull().sum()}')
    return df

if __name__ == "__main__":
    books   = extract_csv('data/books.csv')
    ratings = extract_csv('data/ratings.csv')
    tags    = extract_csv('data/tags.csv')
    book_tags = extract_csv('data/book_tags.csv')