--- AUTHORS
CREATE TABLE authors(
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) UNIQUE NOT NULL
);

---  BOOKS
CREATE TABLE books(
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(500) NOT NULL,
    author_id       INT REFERENCES authors(id),
    isbn            VARCHAR(20),
    publication_year INT,
    publisher       VARCHAR(255),
    page_count      INT,
    average_rating  DECIMAL(3,2),
    ratings_count   INT
);

--- GENRES
CREATE TABLE genres(
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) UNIQUE NOT NULL
);

--- BOOK-GENRE JUNCTION
CREATE TABLE book_genres(
    book_id         INT REFERENCES books(id),
    genre_id        INT REFERENCES genres(id),
    PRIMARY KEY (book_id, genre_id)
);

--- RATING
CREATE TABLE ratings(
    id          SERIAL,
    book_id     INT REFERENCES books(id),
    user_id     INT,
    rating      SMALLINT CHECK (rating between 1 and 5),
    PRIMARY KEY (id, rating)
)PARTITION BY RANGE(rating);

CREATE TABLE ratings_low  PARTITION OF ratings FOR VALUES FROM (1) TO (3);
CREATE TABLE ratings_mid  PARTITION OF ratings FOR VALUES FROM (3) TO (4);
CREATE TABLE ratings_high PARTITION OF ratings FOR VALUES FROM (4) TO (6);