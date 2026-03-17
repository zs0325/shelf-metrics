-- Analysis 08: Partitioning Analysis
-- Concept: Partition pruning, performance benefits
-- 
-- Problem:
-- Demonstrate that the ratings table partitioning by rating value provides a measurable query performance benefit.

-- 8a. Query WITHOUT partition pruning — scans all partitions
EXPLAIN ANALYZE
SELECT
    book_id,
    COUNT(*)            AS total_ratings,
    ROUND(AVG(rating::numeric), 2) AS avg_rating
FROM ratings
GROUP BY book_id
ORDER BY total_ratings DESC
LIMIT 10;

/*
->  Parallel Seq Scan on ratings_high ratings_3  (cost=0.00..39415.14 rows=1715714 width=6) (actual time=1.251..476.957 rows=1372571 loops=3)
->  Parallel Seq Scan on ratings_mid ratings_2  (cost=0.00..13112.39 rows=570739 width=6) (actual time=1.322..299.263 rows=684887 loops=2)
->  Parallel Seq Scan on ratings_low ratings_1  (cost=0.00..5453.47 rows=284147 width=6) (actual time=0.585..393.122 rows=483050 loops=1)
 Planning Time: 7.694 ms
 Execution Time: 1910.561 ms
*/


-- 8b. Query WITH partition pruning — scans only ratings_high
EXPLAIN ANALYZE
SELECT
    book_id,
    COUNT(*)    AS total_ratings
FROM ratings
WHERE rating >= 4
GROUP BY book_id
ORDER BY total_ratings DESC
LIMIT 10;

/*
->  Parallel Seq Scan on ratings_high ratings  (cost=0.00..43704.42 rows=1715714 width=4) (actual time=0.411..302.223 rows=1372571 loops=3)
    Filter: (rating >= 4)
 Planning Time: 4.171 ms
 Execution Time: 670.398 ms
*/


-- 8c. Querying a specific partition directly
EXPLAIN ANALYZE
SELECT
    b.title,
    a.name          AS author,
    COUNT(*)        AS low_ratings_count
FROM ratings_low r
JOIN books b    ON r.book_id = b.id
JOIN authors a  ON b.author_id = a.id
GROUP BY b.title, a.name
ORDER BY low_ratings_count DESC
LIMIT 20;

/*
->  Seq Scan on ratings_low r  (cost=0.00..7442.50 rows=483050 width=4) (actual time=0.011..37.374 rows=483050 loops=1)
 Planning Time: 1.448 ms
 Execution Time: 516.299 ms
*/