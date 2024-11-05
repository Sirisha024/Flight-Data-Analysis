# FlightDataAnalysis

FlightDataAnalysis is a project designed to analyze flight history data to identify the best times to fly, aiming to minimize delays. The project leverages Databricks, an S3 bucket for data storage, and a combination of SQL and Python (PySpark) for data processing. It follows the Medallion Architecture, organizing data into bronze, silver, and gold tables for a structured, step-by-step analysis approach.

## Project Overview
This project performs data ingestion, transformation, and analysis on flight history data. By creating tables with cleaned and well-structured data, aimed to uncover insights into optimal flight times across various time frames. The architecture used allows for efficient querying, transformations, and storage.

### Technologies Used
- **Databricks**: For scalable data processing and analysis.
- **S3 Bucket**: Data storage location for CSV datasets.
- **Medallion Architecture**: Ensures organized data layers (bronze, silver, and gold) for a cleaner and more reliable pipeline.
- **SQL and Python (PySpark)**: SQL for data querying and manipulation; Python for data cleaning and transformations.

## Steps and Operations

### Step 1: Creating the Database
   - **Database Name**: `FlightDataAnalysis`
   - **Purpose**: To store and organize tables for the analysis of flight data.

### Step 2: Mounting CSV Data from S3 Bucket
   - **Dataset**: Flight history, airport details, and airline details.
   - **Location**: Mounted to the Databricks environment from an S3 bucket, making it accessible for data processing.

### Step 3: Creating Bronze Tables (External Tables)
   - **Flight History Table**: Stores raw flight data from CSV files.
   - **Airport Details Table**: Contains airport information linked to flights.
   - **Airline Details Table**: Stores data on airlines, supporting additional context in analysis.
   - **Purpose**: Bronze tables act as raw data storage, retaining the original data structure for reference.

### Step 4: Creating the Bronze Delta Clean Table
   - **Objective**: Clean the flight data by adjusting data types and removing unnecessary columns.
   - **Actions**:
     - Changing data types of columns using PySpark’s `cast` function for compatibility and optimization.
     - Converting any "NA" values to `NULL` (Spark SQL does not handle "NA" as `NULL` by default).
     - Creating a clean Delta table with standardized column names and data types.
   - **Output**: `Bronze_Delta_Clean_Table`, a refined table that’s optimized for further transformations.

### Step 5: Creating Silver Table
   - **Objective**: Further clean the data by handling null values and fixing column inconsistencies.
   - **Actions**:
     - Updating columns with null values using CASE statements, ensuring data quality.
     - Verifying transformations with a cross-check process.
   - **Output**: A structured Delta table with refined columns, prepared for in-depth analysis.

### Step 6: Installing Nutter for Unit Testing
   - **Purpose**: Ensures the reliability and accuracy of the data processing pipeline.
   - **Tool**: Nutter, a testing framework for Databricks notebooks, is used for writing unit tests on the data transformation processes.

### Step 7: Creating the Gold Table
   - **Objective**: To create an optimized final table containing insights on flight patterns, delays, and optimal travel times.
   - **Features**:
     - Aggregated data ready for analysis on flight delays by day, week, and time of year.
     - Structured for efficient querying and business intelligence applications.

## Analysis Goal
The analysis is centered around identifying:
- Best times of day, week, and year to fly with minimal delays.
- Patterns in delays across different airports and airlines.

## Architecture
This project uses the **Medallion Architecture**:
- **Bronze Layer**: Raw data stored in external tables, directly from the source.
- **Silver Layer**: Intermediate data with cleaned and standardized data types and column structures.
- **Gold Layer**: Final table with analytical data, ready for reporting and insights.

