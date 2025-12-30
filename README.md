# SQL-Global-Layoffs-Analysis
Data cleaning and exploratory data analysis of global layoffs using MySQL
## Project Overview
This project analyzes global layoff data to uncover trends, patterns, and insights into the economic landscape affecting various industries. Using MySQL, I performed end-to-end data processing, from raw data cleaning to advanced exploratory data analysis (EDA).

**Dataset:** [Layoffs 2022 Dataset](https://www.kaggle.com/datasets/swaptr/layoffs-2022)


## 🛠️ Tech Stack
* **SQL Flavor:** MySQL
* **Key Techniques:**
    * Data Cleaning (Handling duplicates, nulls, standardization)
    * CTEs (Common Table Expressions)
    * Window Functions (`ROW_NUMBER`, `DENSE_RANK`, `SUM OVER`)
    * Aggregations & Rolling Totals

## 🔍 Key Steps
1.  **Data Cleaning**:
    * Removed duplicate entries to ensure data integrity.
    * Standardized industry names (e.g., merging 'Crypto Currency' and 'CryptoCurrency').
    * Populated missing null values using self-joins.
    * Removed irrelevant columns and rows.

2.  **Exploratory Data Analysis (EDA)**:
    * Identified companies with the largest single-day layoffs.
    * Analyzed layoff trends by **Industry**, **Country**, and **Year**.
    * Calculated rolling totals of layoffs month-over-month.
    * Ranked top companies by layoffs per year using window functions.

## 📈 Key Insights & Sample Queries
*(See the `DataCleaning&EDA.sql` file for the full code)*

**Top Industries Affected:**
The analysis revealed that the Consumer and Retail sectors faced significant layoffs, followed closely by the Tech industry.

**Rolling Total Analysis:**
By calculating a rolling total, we observed a sharp increase in layoffs starting in late 2022, continuing into 2023.

## 🤝 Acknowledgements
This project was completed as part of a guided learning experience by **Alex The Analyst**. The SQL queries and analysis logic follow his "Data Analytics Bootcamp" tutorial series.
* **Tutorial Reference:** [Alex The Analyst - Full SQL Project](https://www.youtube.com/playlist?list=PLUaB-1hjhk8FE_XZ87vPPSfHqb6OcM0cF)




