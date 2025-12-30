-- SQL Project: Data Cleaning & Exploratory Data Analysis (EDA)
-- Project Goal: Clean raw housing data and perform exploratory analysis.
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- =========================================================================================================
-- PART 1: DATA CLEANING
-- =========================================================================================================

-- ==========================================
-- Step 1: Reset Environment
-- (Prevent conflicts with existing tables)
-- ==========================================
DROP TABLE IF EXISTS world_layoffs.layoffs;
DROP TABLE IF EXISTS world_layoffs.layoffs_staging;

-- ==========================================
-- Step 2: Create Raw Table
-- (Set all columns to TEXT temporarily to avoid import errors)
-- ==========================================
CREATE TABLE world_layoffs.layoffs (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off TEXT,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions TEXT
);

-- ==========================================
-- Step 3: Load Data
-- (Importing data from local CSV file)
-- ==========================================
LOAD DATA LOCAL INFILE '/Users/wangyichen/Downloads/layoffs.csv' 
INTO TABLE world_layoffs.layoffs 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

-- Check raw data row count (Expected: 2361)
SELECT COUNT(*) AS raw_data_count FROM world_layoffs.layoffs;

-- ==========================================
-- Step 4: Create Staging Table
-- (This is the table where we will perform the cleaning operations)
-- ==========================================
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- ==========================================
-- Step 5: Data Cleaning Process
-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Handle Null Values
-- 4. Remove Unnecessary Columns/Rows
-- ==========================================

SELECT * FROM world_layoffs.layoffs_staging;

-- ------------------------------------------
-- 1. Remove Duplicates
-- ------------------------------------------

-- First, let's check for duplicates
SELECT *
FROM world_layoffs.layoffs_staging;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Verify duplicates using row numbers
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- Let's check 'Oda' to confirm if they are real duplicates
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';
-- Note: It looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate.

-- These are our real duplicates (checking all columns)
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- These are the ones we want to delete where the row number is > 1

-- ---------------------------------------------------
-- Strategy: Create a new staging table with a row_num column
-- This allows us to easily filter and delete duplicates (where row_num >= 2)
-- ---------------------------------------------------

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert data into the new table, calculating row numbers based on all columns
INSERT INTO world_layoffs.layoffs_staging2
SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Verify the duplicates before deleting
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- Disable safe updates to allow delete
SET SQL_SAFE_UPDATES = 0;

-- Delete duplicates
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;
-- ------------------------------------------
-- 2. Standardize Data
-- ------------------------------------------

SELECT * FROM world_layoffs.layoffs_staging2;

-- Check industry column for nulls and empty rows
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Investigating specific companies
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Logic: If there is another row with the same company name, update it to the non-null industry values.
-- First, set blanks to NULLs since those are typically easier to work with.
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Check if they are all null now
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Populate those nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Check if Bally's is the only one left (since it had no populated row to copy from)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Standardize 'Crypto' industry variations
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Verify Crypto fix
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Standardize Country names (Fix trailing periods in 'United States')
SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Verify Country fix
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Fix Date columns
SELECT *
FROM world_layoffs.layoffs_staging2;

-- Use STR_TO_DATE to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2;

-- ------------------------------------------
-- 3. Handle Null Values
-- ------------------------------------------

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that.
-- I like having them null because it makes it easier for calculations during the EDA phase.
-- So there isn't anything I want to change with the null values.

-- ------------------------------------------
-- 4. Remove Unnecessary Columns and Rows
-- ------------------------------------------

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM world_layoffs.layoffs_staging2;

-- Drop the helper column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM world_layoffs.layoffs_staging2;


-- =========================================================================================================
-- PART 2: EXPLORATORY DATA ANALYSIS (EDA)
-- =========================================================================================================
-- Goal: Explore the data to find trends, patterns, or outliers.

-- Here we are just going to explore the data and find trends or patterns or anything interesting like outliers
-- Normally when you start the EDA process you have some idea of what you're looking for
-- With this info we are just going to look around and see what we find!

SELECT * FROM world_layoffs.layoffs_staging2;

-- Easier Queries
DESCRIBE world_layoffs.layoffs_staging2;

-- Convert Data Types for Analysis (EDA Preparation)
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN total_laid_off INT;

-- Convert funds_raised_millions to Integer
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN funds_raised_millions INT;

-- Convert percentage_laid_off to Float
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN percentage_laid_off FLOAT;

-- Key Metrics
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- These are mostly startups it looks like who all went out of business during this time

-- If we order by funds_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt looks like an EV company, Quibi! I recognize that company - wow raised like 2 billion dollars and went under - ouch

-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY -------------------------------------------

-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging2
ORDER BY 2 DESC
LIMIT 5;
-- Now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- By location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Total in the past 3 years or in the dataset by Country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Total by Year
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- Total by Industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total by Stage
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- TOUGHER QUERIES ----------------------------------------------------------------------

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at:

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- Now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;