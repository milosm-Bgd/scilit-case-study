-- SELECT database();

-- DDL CREATE TABLES 

-- CREATE DATABASE IF NOT EXISTS scilit_db_test;

-- USE scilit_db_test;

DROP TABLE IF EXISTS dates_test;
CREATE TABLE dates_test (
  date_id INT NOT NULL,
  year INT,
  month INT,
  day INT,
  PRIMARY KEY (date_id)
);

DROP TABLE IF EXISTS sources_test;
CREATE TABLE sources_test (
  source_id INT NOT NULL,
  source_name VARCHAR(255),
  PRIMARY KEY (source_id)
);

DROP TABLE IF EXISTS authors_test;
CREATE TABLE authors_test (
  author_id INT NOT NULL,
  given_name VARCHAR(100),
  family_name VARCHAR(100),
  affiliation VARCHAR(255),
  PRIMARY KEY (author_id)
);

DROP TABLE IF EXISTS publications_test;
CREATE TABLE publications_test (
  publication_id INT NOT NULL,
  doi VARCHAR(255),
  title VARCHAR(255),
  publisher VARCHAR(255),
  type VARCHAR(50),
  source_id INT,
  issued_date_id INT,
  PRIMARY KEY (publication_id),
  FOREIGN KEY (source_id) REFERENCES sources_test(source_id),
  FOREIGN KEY (issued_date_id) REFERENCES dates_test(date_id)
);


DROP TABLE IF EXISTS publication_authors_test;
CREATE TABLE publication_authors_test (
  publication_id INT,
  author_id INT,
  sequence INT,
  PRIMARY KEY (publication_id, author_id),
  FOREIGN KEY (publication_id) REFERENCES publications_test(publication_id),
  FOREIGN KEY (author_id) REFERENCES authors_test(author_id)
  );