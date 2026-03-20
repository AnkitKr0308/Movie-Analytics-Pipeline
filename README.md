# Movie Analytics Pipeline

## Project Overview

The **Movie Analytics Pipeline** is an end-to-end data engineering project built using Python, Pandas, and SQL. This project simulates a real-world ETL (Extract, Transform, Load) pipeline that fetches movie data from external sources, cleans and transforms it, and loads it into a relational database for analytics. It is designed to demonstrate practical data engineering skills, including data ingestion, transformation, data modeling, and database integration.

---

## Features

- **Data Ingestion**
  - Fetch movie details from **The Movie Database (TMDb) API**.
  - Read additional movie datasets from CSV files.
  - Handle JSON responses and nested data structures.
- **Data Transformation**
  - Clean and standardize data using **Pandas**.
  - Merge multiple datasets into unified tables.
  - Perform aggregations and generate analytics-ready tables.
  - Handle missing values, duplicates, and inconsistent data.
  - Extract and transform datetime fields for time-series analysis.
- **Data Loading**
  - Design relational schema for **Movies, Production_Companies, and Genres** tables.
  - Load transformed data into a SQL Server database.
  - Ensure normalized database structure with relationships.
- **Analytics**
  - Sample queries to derive insights:
    - Top 10 highest-grossing movies per year.
    - Average ratings by genre.
    - Monthly trending movies and revenue analysis.

- **Modular Project Structure**
  - Separate scripts for ingestion, transformation, and database loading.
  - Configurations, including API keys, stored securely in a `.env` file.

---

## Tech Stack

- **Python** (Data ingestion and transformations)
- **Pandas** (Data manipulation)
- **Requests** (API calls)
- **SQL** (Data storage and analytics)
- **dotenv** (Secure configuration management)
- Optional: Airflow for workflow orchestration

---
