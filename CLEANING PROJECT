-- =====================================================
-- SQL Project: Data Cleaning
-- Dataset: Layoffs 2022 (Kaggle)
-- =====================================================

-- Step 0. Inspect raw data
SELECT * 
FROM world_layoffs.layoffs;

-- Step 1. Create a staging table to preserve raw data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- =====================================================
-- Step 2. Identify and remove duplicates
-- =====================================================

-- Check for duplicates based on selected columns
SELECT company, industry, total_laid_off, `date`,
	ROW_NUMBER() OVER (
		PARTITION BY company, industry, total_laid_off, `date`
	) AS row_num
FROM world_layoffs.layoffs_staging;

-- Extended duplicate check across all relevant columns
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Create a new staging table with row numbers for deduplication
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
	`company` TEXT,
	`location` TEXT,
	`industry` TEXT,
	`total_laid_off` INT,
	`percentage_laid_off` TEXT,
	`date` TEXT,
	`stage` TEXT,
	`country` TEXT,
	`funds_raised_millions` INT,
	`row_num` INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`, `location`, `industry`, `total_laid_off`,
 `percentage_laid_off`, `date`, `stage`, `country`,
 `funds_raised_millions`, `row_num`)
SELECT `company`, `location`, `industry`, `total_laid_off`,
       `percentage_laid_off`, `date`, `stage`, `country`,
       `funds_raised_millions`,
       ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Remove duplicate records (keep only row_num = 1)
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- =====================================================
-- Step 3. Standardize data
-- =====================================================

-- Standardize industry values
-- Replace empty strings with NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate NULL industries based on other rows from the same company
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Unify Crypto-related labels into one category
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names (remove trailing dots)
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Convert date strings to DATE type
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` IS NOT NULL;

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- =====================================================
-- Step 4. Handle NULL values
-- =====================================================

-- Keep NULL values in numeric fields for accurate analysis
-- (total_laid_off, percentage_laid_off, funds_raised_millions)

-- =====================================================
-- Step 5. Remove irrelevant rows and columns
-- =====================================================

-- Delete rows where both total_laid_off and percentage_laid_off are NULL
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Remove helper column row_num
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- Final cleaned dataset
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- =====================================================
-- End of Cleaning Process
-- =====================================================
