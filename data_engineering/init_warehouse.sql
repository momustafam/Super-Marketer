CREATE DATABASE CustomerWarehouse;

USE CustomerWarehouse;

-- Drop tables if they exist (optional for dev)
IF OBJECT_ID('dbo.fact_trans', 'U') IS NOT NULL DROP TABLE dbo.fact_trans;
IF OBJECT_ID('dbo.dim_card', 'U') IS NOT NULL DROP TABLE dbo.dim_card;
IF OBJECT_ID('dbo.dim_users', 'U') IS NOT NULL DROP TABLE dbo.dim_users;


-- Create dimension tables
CREATE TABLE dbo.dim_users(
    user_id INTEGER PRIMARY KEY,
    age INTEGER,
    birth_month INTEGER,
    birth_year INTEGER,
    gender TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    latitude REAL,
    longitude REAL,
    yearly_income REAL,
    num_credit_cards INTEGER
);


-- Create dim_card table
CREATE TABLE dim_card (
    card_id INTEGER PRIMARY KEY,
    card_type TEXT
);


-- Create fact_trans table
CREATE TABLE fact_trans (
    id BIGINT PRIMARY KEY ,
    user_id INTEGER,
    card_id INTEGER,
    trans_timestamp TEXT,
    trans_amount REAL,
    mcc TEXT,
    FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
    FOREIGN KEY (card_id) REFERENCES dim_card(card_id)
);

-- Bulk load static CSV data
-- Ensure files are mounted to /var/opt/mssql/data/

-- Load Users
BULK INSERT dbo.dim_users
FROM '/data/dim_users.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
TABLOCK
);


-- Load Products
BULK INSERT dbo.dim_card
FROM '/data/dim_cards.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
TABLOCK
);


