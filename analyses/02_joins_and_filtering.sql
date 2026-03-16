-- Analysis 02: Joins and Filtering
-- Concept: Multi-table JOINs, WHERE filters

-- 2a. Books with author name and genre, rated above 4.0 with at least 1000 ratings for statistical credibility
SELECT
    b.title,
    a.name          AS author,
    STRING_AGG(g.name, ', ')  AS genre,
    b.average_rating,
    b.ratings_count
FROM books b
JOIN authors a      ON b.author_id = a.id
JOIN book_genres bg ON b.id = bg.book_id
JOIN genres g       ON bg.genre_id = g.id
WHERE b.average_rating > 4.0
AND   b.ratings_count  >= 1000
GROUP BY b.title, a.name, b.average_rating, b.ratings_count
ORDER BY b.average_rating DESC;


-- 2b. Author with the most books in the database
SELECT
    a.name              AS author,
    COUNT(b.id)         AS total_books
FROM authors a
JOIN books b ON a.id = b.author_id
GROUP BY a.name
ORDER BY total_books DESC
LIMIT 1;


-- 2c. Books published between 1990 and 2010 with their genres
SELECT
    b.title,
    b.publication_year,
    a.name      AS author,
    STRING_AGG(g.name, ', ')      AS genre
FROM books b
JOIN authors a      ON b.author_id = a.id
JOIN book_genres bg ON b.id = bg.book_id
JOIN genres g       ON bg.genre_id = g.id
WHERE b.publication_year BETWEEN 1990 AND 2010
GROUP BY b.title, b.publication_year, a.name
ORDER BY b.publication_year DESC;