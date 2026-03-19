CREATE DATABASE MovieAnalytics 
GO

CREATE SCHEMA staging
GO

USE MovieAnalytics
GO

DROP TABLE IF EXISTS movies
GO

CREATE TABLE movies(
	id INT,
	title NVARCHAR(500),
	release_date DATETIME2,
	vote_average INT,
	vote_count INT,
	CONSTRAINT PK_id PRIMARY KEY(id),
)
GO

DROP TABLE IF EXISTS genres
GO

CREATE TABLE genres(
	genre_id INT,
	movie_id INT,
	genre_name NVARCHAR(50),
	--CONSTRAINT FK_movie_id_movies_id FOREIGN KEY (movie_id) REFERENCES movies(id),
	CONSTRAINT PK_genre_id PRIMARY KEY (genre_id),
	CONSTRAINT UQ_genre_id UNIQUE (genre_id)
)
GO

DROP TABLE IF EXISTS production_companies
GO

CREATE TABLE production_companies(
	company_id INT,
	company_name NVARCHAR(50),
	movie_id INT,
	CONSTRAINT PK_company_id PRIMARY KEY(company_id),
	CONSTRAINT UQ_company_id UNIQUE (company_id)
	--CONSTRAINT FK_movie_id_movies_id FOREIGN KEY(movie_id) REFERENCES movies(id)
)
GO

DROP TABLE IF EXISTS movie_genre
GO

CREATE TABLE movie_genre(
	movie_id INT,
	genre_id INT,
	--CONSTRAINT FK_movie_id_movies_id FOREIGN KEY (movie_id) REFERENCES movies(id),
	--CONSTRAINT FK_genre_id_genres_genre_id FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
)
GO

DROP TABLE IF EXISTS movie_companies
GO

CREATE TABLE movie_companies(
	movie_id INT,
	company_id INT,
	--CONSTRAINT FK_movie_id_movies_id FOREIGN KEY (movie_id) REFERENCES movies(id),
	--CONSTRAINT FK_company_id_company_company_id FOREIGN KEY (company_id) REFERENCES production_companies(company_id)
)
GO