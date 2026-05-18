-- =========================================================
-- TMDB ANALYTICS QUERIES
-- =========================================================


-- 1. Top 10 Highest Rated Content
SELECT
    content_title,
    media_type,
    vote_average,
    popularity
FROM tmdb_gold
ORDER BY vote_average DESC, popularity DESC
LIMIT 10;



-- 2. Most Popular Movies
SELECT
    content_title,
    popularity,
    vote_count
FROM tmdb_gold
WHERE media_type = 'movie'
ORDER BY popularity DESC
LIMIT 10;



-- 3. Most Popular TV Shows
SELECT
    content_title,
    popularity,
    vote_count
FROM tmdb_gold
WHERE media_type = 'tv'
ORDER BY popularity DESC
LIMIT 10;



-- 4. Average Rating By Media Type
SELECT
    media_type,
    ROUND(AVG(vote_average), 2) AS avg_rating
FROM tmdb_gold
GROUP BY media_type;



-- 5. Total Content Count By Language
SELECT
    original_language,
    COUNT(*) AS total_content
FROM tmdb_gold
GROUP BY original_language
ORDER BY total_content DESC;



-- 6. Highly Rated Content
SELECT
    content_title,
    media_type,
    vote_average
FROM tmdb_gold
WHERE vote_average >= 8
ORDER BY vote_average DESC;



-- 7. Most Voted Content
SELECT
    content_title,
    vote_count,
    vote_average
FROM tmdb_gold
ORDER BY vote_count DESC
LIMIT 10;



-- 8. Average Popularity By Media Type
SELECT
    media_type,
    ROUND(AVG(popularity), 2) AS avg_popularity
FROM tmdb_gold
GROUP BY media_type;



-- 9. Daily Trending Content Count
SELECT
    _snapshot_date,
    COUNT(*) AS total_records
FROM tmdb_gold
GROUP BY _snapshot_date
ORDER BY _snapshot_date DESC;



-- 10. Top Languages By Average Rating
SELECT
    original_language,
    ROUND(AVG(vote_average), 2) AS avg_rating
FROM tmdb_gold
GROUP BY original_language
HAVING COUNT(*) >= 2
ORDER BY avg_rating DESC;