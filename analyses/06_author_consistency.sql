-- Analysis 06: Author Consistency
-- Concept: Subqueries, STDDEV, variance analysis

-- 6a. Most consistent authors — lowest rating variance
SELECT
    author,
    total_books,
    avg_rating,
    consistency_score
FROM (
    SELECT
        a.name                              AS author,
        COUNT(b.id)                         AS total_books,
        ROUND(AVG(b.average_rating), 2)     AS avg_rating,
        ROUND(STDDEV(b.average_rating), 3)  AS consistency_score
    FROM authors a
    JOIN books b ON a.id = b.author_id
    GROUP BY a.name
    HAVING COUNT(b.id) >= 3
) author_stats
WHERE consistency_score IS NOT NULL
ORDER BY consistency_score ASC
LIMIT 20;


-- 6b. Most inconsistent authors — highest rating variance these are authors whose books divide readers most
SELECT
    author,
    total_books,
    avg_rating,
    consistency_score
FROM (
    SELECT
        a.name                              AS author,
        COUNT(b.id)                         AS total_books,
        ROUND(AVG(b.average_rating), 2)     AS avg_rating,
        ROUND(STDDEV(b.average_rating), 3)  AS consistency_score
    FROM authors a
    JOIN books b ON a.id = b.author_id
    GROUP BY a.name
    HAVING COUNT(b.id) >= 3
) author_stats
WHERE consistency_score IS NOT NULL
ORDER BY consistency_score DESC
LIMIT 20;


-- 6c. Consistency breakdown by genre which genres produce the most consistently rated books
SELECT
    g.name                              AS genre,
    COUNT(DISTINCT b.title)                AS total_books,
    ROUND(AVG(b.average_rating), 2)     AS avg_rating,
    ROUND(STDDEV(b.average_rating), 3)  AS rating_stddev
FROM genres g
JOIN book_genres bg ON g.id = bg.genre_id
JOIN books b        ON bg.book_id = b.id
GROUP BY g.name
ORDER BY rating_stddev ASC;