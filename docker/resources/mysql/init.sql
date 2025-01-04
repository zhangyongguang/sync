-- Create source_db and target_db in MySQL
CREATE DATABASE IF NOT EXISTS source_db;
CREATE DATABASE IF NOT EXISTS target_db;

-- Use source_db
USE source_db;

-- Create table users
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
insert into users (id, name, email) values (1, 'John', 'John@mail' );

-- Use target_db
USE target_db;

-- Create table users
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
