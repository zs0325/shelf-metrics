-- Analysis 05: Rating Distribution
-- Concept: CASE, COUNT, percentage breakdown per genre
--
-- Problem:
-- Show how ratings are distributed across genres as a percentage

SELECT
    g.name                                                      AS genre,
    COUNT(r.rating)                                             AS total_ratings,
    ROUND(100.0 * SUM(CASE WHEN r.rating = 1 THEN 1 ELSE 0 END) / COUNT(r.rating), 1) AS pct_1_star,
    ROUND(100.0 * SUM(CASE WHEN r.rating = 2 THEN 1 ELSE 0 END) / COUNT(r.rating), 1) AS pct_2_star,
    ROUND(100.0 * SUM(CASE WHEN r.rating = 3 THEN 1 ELSE 0 END) / COUNT(r.rating), 1) AS pct_3_star,
    ROUND(100.0 * SUM(CASE WHEN r.rating = 4 THEN 1 ELSE 0 END) / COUNT(r.rating), 1) AS pct_4_star,
    ROUND(100.0 * SUM(CASE WHEN r.rating = 5 THEN 1 ELSE 0 END) / COUNT(r.rating), 1) AS pct_5_star
FROM genres g
JOIN book_genres bg ON g.id  = bg.genre_id
JOIN books b        ON bg.book_id = b.id
JOIN ratings r      ON b.id  = r.book_id
GROUP BY g.name
ORDER BY pct_5_star DESC;