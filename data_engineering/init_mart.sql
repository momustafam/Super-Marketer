-- Create the Marketing Data Mart database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'MarketingDataMart')
BEGIN
    CREATE DATABASE MarketingDataMart;
    PRINT '✅ Database "MarketingDataMart" created.';
END
ELSE
BEGIN
    PRINT 'ℹ️ Database "MarketingDataMart" already exists.';
END
GO

-- Switch to the new database
USE MarketingDataMart;
GO

-- Table 1: Age Clusters
IF OBJECT_ID('age_count', 'U') IS NULL
BEGIN
CREATE TABLE age_count (
                           age INT,
                           count INT,
                           cluster VARCHAR(50)
);
PRINT '✅ Created table: age_count';
END
GO

-- Table 2: Gender Count
IF OBJECT_ID('gender_count', 'U') IS NULL
BEGIN
CREATE TABLE gender_count (
                              gender VARCHAR(20),
                              count INT,
                              cluster VARCHAR(50)
);
PRINT '✅ Created table: gender_count';
END
GO

-- Table 3: Service Count
IF OBJECT_ID('service_count', 'U') IS NULL
BEGIN
CREATE TABLE service_count (
    [service] VARCHAR(100),
    count INT
);
PRINT '✅ Created table: service_count';
END
GO

-- Table 4: User Geo Distribution
IF OBJECT_ID('user_geo', 'U') IS NULL
BEGIN
CREATE TABLE user_geo (
                          longitude FLOAT,
                          latitude FLOAT,
                          city VARCHAR(100),
                          country VARCHAR(100),
                          cluster VARCHAR(50)
);
PRINT '✅ Created table: user_geo';
END
GO

-- Table 5: Transactions by Day
IF OBJECT_ID('trans_by_day', 'U') IS NULL
BEGIN
CREATE TABLE trans_by_day (
                              hour INT,
                              count INT
);
PRINT '✅ Created table: trans_by_day';
END
GO

-- Table 6: Stats
IF OBJECT_ID('stats', 'U') IS NULL
BEGIN
CREATE TABLE stats (
                       new_users INT,
                       returning_users INT,
                       new_transactions INT,
                       avg_clv FLOAT
);
PRINT '✅ Created table: stats';
END
GO
