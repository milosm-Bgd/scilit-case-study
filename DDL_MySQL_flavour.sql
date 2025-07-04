-- DDL CREATE TABLE 

CREATE DATABASE IF NOT EXISTS scilit_db;
    
USE scilit_db;

   """
        DROP TABLE IF EXISTS dates;

        CREATE TABLE dates (
         date_id INT NOT NULL,
         year INT,
         month INT,
         day INT,
         PRIMARY KEY (date_id)
        )"""
        ,
        """
        DROP TABLE IF EXISTS sources;

        CREATE TABLE sources (
         source_id INT NOT NULL,
         source_name VARCHAR(255),
         PRIMARY KEY (source_id)
        )"""
        ,
        """
        DROP TABLE IF EXISTS authors;

        CREATE TABLE authors (
         author_id INT NOT NULL,
         given_name VARCHAR(100),
         family_name VARCHAR(100),
         affiliation VARCHAR(255),
         PRIMARY KEY (author_id)
        )"""
        ,
        """
        DROP TABLE IF EXISTS publications;

        CREATE TABLE publications (
         publication_id INT NOT NULL,
         doi VARCHAR(255),
         title VARCHAR(255),
         publisher VARCHAR(255),
         type VARCHAR(50),
         source_id INT,
         issued_date_id INT,
         PRIMARY KEY (publication_id),
         FOREIGN KEY (source_id) REFERENCES sources(source_id),
         FOREIGN KEY (issued_date_id) REFERENCES dates(date_id)
        )"""
        ,
        """
        DROP TABLE IF EXISTS publication_authors;

        CREATE TABLE publication_authors (
         publication_id INT,
         author_id INT,
         sequence INT,
         PRIMARY KEY (publication_id, author_id),
         FOREIGN KEY (publication_id) REFERENCES publications(publication_id),
         FOREIGN KEY (author_id) REFERENCES authors(author_id)
        )"""
