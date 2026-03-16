-- Analysis 04: Window Functions
-- Concept: RANK(), DENSE_RANK(), running averages

-- 4a. Authors ranked within each genre by average rating minimum 2 books in that genre to qualify
SELECT
    genre,
    author,
    avg_rating,
    total_books,
    RANK()       OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY genre ORDER BY avg_rating DESC) AS dense_rank
FROM (
    SELECT
        g.name                          AS genre,
        a.name                          AS author,
        ROUND(AVG(b.average_rating), 2) AS avg_rating,
        COUNT(b.id)                     AS total_books
    FROM authors a
    JOIN books b        ON a.id = b.author_id
    JOIN book_genres bg ON b.id = bg.book_id
    JOIN genres g       ON bg.genre_id = g.id
    GROUP BY g.name, a.name
    HAVING COUNT(b.id) >= 2
) author_genre_stats
ORDER BY genre, rank;


-- 4b. Running average rating of books ordered by publication year shows whether newer books trend higher or lower in ratings
SELECT
    publication_year,
    title,
    average_rating,
    ROUND(
        AVG(average_rating) OVER (
            ORDER BY publication_year
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ), 2
    ) AS rolling_avg_5_books
FROM books
WHERE publication_year BETWEEN 1950 AND 2024
AND   ratings_count >= 100
ORDER BY publication_year;