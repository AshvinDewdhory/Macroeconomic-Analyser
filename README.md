# Macroeconomic-Analyser

## Data sources
CPI Timeseries.csv
World bank data

## Technologies
1. JavaScript and MySql database: They work well together in a data pipeline: with Node.js, handles asynchronous data processing and API interactions tasks, while MySQL provides a structured backend for efficient data storage and retrieval.
2. PowerBI (recommended)

## Design implementation considerations
1. Data source integration: 
    - Understand the World Bank API structure and endpoints and handle pagination
    - Ensure that the CSV file follows a consistent structure
2. Data transformation:
    - Create a mapping between the data from the World Bank API and the local CSV file to ensure consistency in the schema
3. Error handling and logging

## Data handling
The ETL (extract, transform and load) process is used to combine data from the Corruption Perception Index Data Set and the World Development Indicators into a single, consistent data store that is loaded into a MySQL database. Two required tables are created namely countries and indicator_info having a one-to-many relationship on the country code column.

## PowerBI report
https://app.powerbi.com/view?r=eyJrIjoiOTQxYTY0MDQtOTM1NS00MWI1LTk0M2MtNmVjYjVhMjJmZGU4IiwidCI6IjBhZjA4MjU1LWM3NmItNDhjMC04ZGMyLTBkZmRiZjdiYWU0MCJ9
