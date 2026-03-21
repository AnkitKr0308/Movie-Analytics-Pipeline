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
	created_at DATETIME2,
	updated_at DATETIME2,
	CONSTRAINT PK_id PRIMARY KEY(id)
)
GO

DROP TABLE IF EXISTS genres
GO

CREATE TABLE genres(
	genre_id INT,
	genre_name NVARCHAR(50),
	created_at DATETIME2,
	updated_at DATETIME2,
	CONSTRAINT PK_genre_id PRIMARY KEY (genre_id),
)
GO

DROP TABLE IF EXISTS production_companies
GO

CREATE TABLE production_companies(
	company_id INT,
	company_name NVARCHAR(500),
	created_at DATETIME2,
	updated_at DATETIME2,
	CONSTRAINT PK_company_id PRIMARY KEY(company_id),
)
GO


DROP TABLE IF EXISTS movie_genre
GO

CREATE TABLE movie_genre(
	movie_id INT,
	genre_id INT,
	created_at DATETIME2,
	updated_at DATETIME2
)
GO

DROP TABLE IF EXISTS movie_companies
GO

CREATE TABLE movie_companies(
	movie_id INT,
	company_id INT,
	created_at DATETIME2,
	updated_at DATETIME2
)
GO