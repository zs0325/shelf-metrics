--Analysis 03: Top N Per Group
-- Concept: Window Functions — ROW_NUMBER()
--
-- Problem:
-- Find the top 5 highest rated books in each genre. Only consider books with at least 500 ratings

SELECT
    genre,
    title,
    author,
    average_rating,
    ratings_count,
    rank
FROM (
    SELECT
        g.name                  AS genre,
        b.title,
        a.name                  AS author,
        b.average_rating,
        b.ratings_count,
        ROW_NUMBER() OVER (
            PARTITION BY g.id
            ORDER BY b.average_rating DESC
        )                       AS rank
    FROM books b
    JOIN authors a      ON b.author_id = a.id
    JOIN book_genres bg ON b.id = bg.book_id
    JOIN genres g       ON bg.genre_id = g.id
    WHERE b.ratings_count >= 500
) ranked
WHERE rank <= 5
ORDER BY genre, rank;