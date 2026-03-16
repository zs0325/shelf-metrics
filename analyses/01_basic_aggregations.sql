-- Analysis 01: Basic Aggregations
-- Concept: COUNT, AVG, GROUP BY


-- 1a. Total books per genre ordered by most popular
SELECT
    g.name                  AS genre,
    COUNT(bg.book_id)       AS total_books
FROM genres g
JOIN book_genres bg ON g.id = bg.genre_id
GROUP BY g.name
ORDER BY total_books DESC;


-- 1b. Average rating per genre
SELECT
    g.name                          AS genre,
    ROUND(AVG(b.average_rating), 2) AS avg_rating,
    COUNT(b.id)                     AS book_count
FROM genres g
JOIN book_genres bg ON g.id = bg.genre_id
JOIN books b        ON bg.book_id = b.id
GROUP BY g.name
ORDER BY avg_rating DESC;


-- 1c. Books published per decade
SELECT
    (publication_year / 10) * 10    AS decade,
    COUNT(*)                        AS total_books
FROM books
WHERE publication_year IS NOT NULL
AND   publication_year BETWEEN 1800 AND 2026
GROUP BY decade
ORDER BY decade;