# Duckdb Analysis

This repository contains the implementation of an analysis of a dataset. I created a very large fake dataset and perform data analysis using DuckDB, with a focus on optimizing the query execution time for aggregated queries.The goal of this assignment is to study about the fnctionalities of duckdb and then explore where it shines.

## Dataset Description

The dataset was generated using the Faker library in Python and consists of three tables: transactions, users, and products. The transaction table contains 100 million rows, the users table contains 10 million rows, and the products table contains 200,000 rows. The dataset was structured with the transaction table as the fact table and users and products as dimension tables.

## Analysis Roadmap

1. Installing and Setup of DuckDB
2. Data Loading
3. Basic Data Analysis
   - Data Cleaning
   - Checking for Null Values
   - Checking for Duplicates
4. Aggregate Queries
   - Total Revenue by Category
   - Users with the Highest Total Spending by Country
   - Average Transaction Amount per Day in a Given Time Frame
   - Top Selling Product Categories with Average Price per Category
   - Grouping Users by Age
5. Query Optimization
   - Indexing
   - Window Functions
6. Data Visualization
   - Bar Charts
   - Scatter Plots
   - Histograms

## Key Findings

- The synthetic dataset resembled real-world data scenarios.
- Aggregate queries provided valuable insights into various metrics based on different attributes.
- Optimization techniques significantly improved query performance.

## Database Schema

![Database Schema](images/db_schema.png)

## Execution

To execute the analysis, follow these steps:

1. Install DuckDB: [DuckDB Installation Guide](https://duckdb.org/docs/installation)
2. Clone this repository: `git clone https://github.com/iamhwkchn/analysis_dckdb.git`
3. Download the dataset from this link [data_parquet](https://uofwaterloo-my.sharepoint.com/:f:/r/personal/s36sivar_uwaterloo_ca/Documents/fake_data_transactions?csf=1&web=1&e=fq62cJ) an then place them in the folder merged or create your own dataset by runnig the files in the folder create_db
4. Install required Python packages: `pip install -r requirements.txt`
5. Run the analysis scripts: `python analysis_data.py`
6. The analysis results will be displayed in the console, and visualizations will be saved in the `output/` directory.

Feel free to explore the code and modify the analysis as needed. For any questions or issues, please contact `s36sivar@uwaterloo.ca`.
