-- Example ad-hoc query you can run from Airflow's SQLExecute task or psql
SELECT title, AVG(rating) AS avg_rating, COUNT(*) AS n
FROM fact_movie_ratings
GROUP BY title
ORDER BY avg_rating DESC, n DESC;
