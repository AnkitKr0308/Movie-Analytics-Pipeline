USE MovieAnalytics
GO

DROP VIEW IF EXISTS dbo.genre_collections
GO

-- Count of movies for each genres
CREATE VIEW dbo.genre_collections AS
(
	SELECT 
		g.genre_name,
		COUNT (mg.movie_id) AS number_of_movies
	FROM dbo.movie_genre mg
	LEFT JOIN dbo.genres g
		ON g.genre_id = mg.genre_id
	GROUP BY g.genre_name
)
GO

DROP VIEW IF EXISTS dbo.production_collections
GO

-- Count of movies for each productions
CREATE VIEW dbo.production_collections AS 
(
	SELECT 
		pc.company_name,
		COUNT(mc.movie_id) AS number_of_movies
	FROM dbo.movie_companies mc
	LEFT JOIN dbo.production_companies pc
		ON pc.company_id = mc.company_id
	GROUP BY company_name
)
GO

DROP VIEW IF EXISTS dbo.hit_genres
GO

-- Which genres having highest ratings
CREATE VIEW dbo.hit_genres AS 
(
	SELECT 
		g.genre_name,
		AVG(m.vote_average) AS avg_rating
	FROM dbo.movies m
	JOIN dbo.movie_genre mg
		ON m.id=mg.movie_id
	JOIN dbo.genres g
		ON g.genre_id=mg.genre_id
	GROUP BY g.genre_name
)
GO

DROP VIEW IF EXISTS dbo.hit_productions
GO

-- Which productions having highest ratings
CREATE VIEW dbo.hit_productions AS 
(
	SELECT
		pc.company_name,
		AVG(m.vote_average) AS avg_rating
	FROM dbo.movies m
	JOIN dbo.movie_companies mc
		ON mc.movie_id=m.id
	JOIN dbo.production_companies pc
		ON pc.company_id = mc.company_id
	GROUP BY pc.company_name
)
GO

DROP VIEW IF EXISTS dbo.year_wise_releases
GO

-- Year wise releases
CREATE VIEW dbo.year_wise_releases AS
(
	SELECT
		YEAR(release_date) AS year,
		COUNT(*) AS movie_count
	FROM dbo.movies
	GROUP BY release_date
)
GO

DROP VIEW IF EXISTS dbo.recent_genre_releases
GO

-- recent genre releases
CREATE VIEW dbo.recent_genre_releases AS
(
	SELECT 
		g.genre_name,
		m.title AS movie_name,
		m.release_date
	FROM dbo.movies m
	JOIN movie_genre mg
		ON mg.movie_id = m.id
	JOIN genres g
		ON g.genre_id=mg.genre_id
)
GO

-- Find number of hits, average and flops for each genres
DROP VIEW IF EXISTS dbo.hit_genre_analysis
GO

CREATE VIEW dbo.hit_genre_analysis AS
WITH movie_rating AS
(
	SELECT
		id,
		title,
		CASE 
			WHEN vote_average>6 AND vote_count > 800 THEN 'Hit'
			WHEN vote_average BETWEEN 4 AND 6 AND vote_count BETWEEN 400 AND 800 THEN 'Average'
			ELSE 'Flop'
		END AS ratings
	FROM movies m
)
SELECT 
	g.genre_name,
	m.ratings,
	COUNT(*) AS movie_count
FROM movie_rating m
JOIN movie_genre mg
	ON mg.movie_id = m.id
JOIN genres g
	ON g.genre_id = mg.genre_id
GROUP BY g.genre_name, m.ratings
GO


