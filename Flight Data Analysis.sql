-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook consist of Flight History Data to perform analysis on when is the best time of a day/week of a year to fly to minimize delays. The architecture used in this project is Medallian Architecture.
-- MAGIC
-- MAGIC This notebook is written in SQL and Python so the default cell type is SQL. However, If you needed you can use different languages by using the % Language Name. Databricks support Scala,Python,Sql and R

-- COMMAND ----------

-- Step 1 : Creating new database "FlightDataAnalysis"
Create Database FlightDataAnalysis;

-- COMMAND ----------

-- Using the Database
use FlightDataAnalysis;

-- COMMAND ----------

-- Step 2 : CSV dataset location mounted on S3 bucket
%fs
ls /mnt/siribucket/spring2023/project/flight-history-analysis

-- COMMAND ----------

 -- Step 3 : Creating an external table
 -- Creating a bronze table for Flight- History CSV file (External Table. Data mounted in S3)
  CREATE TABLE Bronze_Flight_Data_Analysis 
  USING csv 
  OPTIONS(path='/mnt/siribucket/spring2023/project/flight-history-analysis/Flight-History-2008.csv',header='true',inferSchema='true');

-- COMMAND ----------

-- Creating a bronze table for Airport Details CSV file (External Table. Data mounted in S3)
CREATE TABLE Bronze_Airport_Details 
USING csv 
OPTIONS(path='/mnt/siribucket/spring2023/project/flight-history-analysis/airports.csv',header='true',inferSchema='true');  

-- COMMAND ----------

-- Creating a bronze table for Airline Details CSV file (External Table. Data mounted in S3)
CREATE TABLE Bronze_Airlines_Details 
USING csv 
OPTIONS(path='/mnt/siribucket/spring2023/project/flight-history-analysis/airlines.csv',header='true',inferSchema='true');

-- COMMAND ----------

-- Describing the Table Bronze_Airport_Details
DESC Bronze_Airport_Details ;

-- COMMAND ----------

-- Describing the Table Bronze_Airlines_Details
DESC Bronze_Airlines_Details ;

-- COMMAND ----------

-- Describing the Table Bronze_Flight_Data_Analysis
DESC TABLE Bronze_Flight_Data_Analysis ;

-- COMMAND ----------

-- displaying the data in the Bronze_Flight_Data_Analysis table with limit 3
SELECT * FROM Bronze_Flight_Data_Analysis LIMIT 3;

-- COMMAND ----------

-- displaying the data in the Bronze_Flight_Data_Analysis table with limit 3
SELECT * FROM Bronze_Airlines_Details LIMIT 3;

-- COMMAND ----------

-- displaying the data in the Bronze_Flight_Data_Analysis table with limit 3
SELECT * FROM Bronze_Airport_Details  LIMIT 3;

-- COMMAND ----------

--Step 4: Creating Bronze Delta EXTERNAL Table (fixing column names, data types, get rid of unwanted columns from CSV/JSON) (Data location in S3.

-- COMMAND ----------

-- creating delta table to fix column names, data types
CREATE TABLE Bronze_Delta_Cleand_Table
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Bronze/Bronze_Delta_Cleand_Table'
AS SELECT * FROM Bronze_Flight_Data_Analysis;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 1. changing the datatypes of column using python (pyspark and cast function) 
-- MAGIC # Note: Sparksql doesnot support Alter function to change the data type of a column
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df = spark.table("Bronze_Delta_Cleand_Table")
-- MAGIC df = df.withColumn("DepTime", col("DepTime").cast("float"))
-- MAGIC df = df.withColumn("ArrTime", col("ArrTime").cast("float"))
-- MAGIC df = df.withColumn("CRSDepTime", col("CRSDepTime").cast("float"))
-- MAGIC df = df.withColumn("CRSArrTime", col("CRSArrTime").cast("float"))
-- MAGIC df = df.withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("integer"))
-- MAGIC df = df.withColumn("AirTime", col("AirTime").cast("integer"))
-- MAGIC df = df.withColumn("TaxiIn", col("TaxiIn").cast("integer"))
-- MAGIC df = df.withColumn("TaxiOut", col("TaxiOut").cast("integer"))
-- MAGIC df = df.withColumn("Cancelled", col("Cancelled").cast("boolean"))
-- MAGIC df = df.withColumn("AirTime", col("AirTime").cast("integer"))
-- MAGIC df = df.withColumn("WeatherDelay", col("WeatherDelay").cast("integer"))
-- MAGIC df = df.withColumn("SecurityDelay", col("SecurityDelay").cast("integer"))
-- MAGIC df = df.withColumn("Diverted", col("Diverted").cast("boolean"))
-- MAGIC df = df.withColumn("LateAircraftDelay", col("LateAircraftDelay").cast("integer"))
-- MAGIC df = df.withColumn("CarrierDelay", col("CarrierDelay").cast("integer"))
-- MAGIC df.write.mode("overwrite").saveAsTable("Bronze_Delta_Clean_Table")
-- MAGIC
-- MAGIC # Note : created and saved the table as Bronze_Delta_Clean_Table after changing the datatype of the above mentioned cloumns

-- COMMAND ----------

-- describing the Bronze_Delta_Clean_Table 
DESC Bronze_Delta_Clean_Table;

-- COMMAND ----------

-- cleaning the data and updating the columns has NA data to Null 
-- Note : Spark SQL does not take NA as null by default so changing all columns that has NA to Null 
-- update statement does not allow multiple SET functions, So here I am  using CASE statement to perform for all columns in one single execution

-- COMMAND ----------

UPDATE Bronze_Delta_Clean_Table
SET 
  DepTime = CASE WHEN DepTime = 'NA' THEN NULL ELSE DepTime END,
  ArrTime = CASE WHEN ArrTime = 'NA' THEN NULL ELSE ArrTime END,
  ActualElapsedTime = CASE WHEN ActualElapsedTime = 'NA' THEN NULL ELSE ActualElapsedTime END,
  AirTime = CASE WHEN AirTime = 'NA' THEN NULL ELSE AirTime END,
  ArrDelay = CASE WHEN ArrDelay = 'NA' THEN NULL ELSE ArrDelay END,
  DepDelay = CASE WHEN DepDelay = 'NA' THEN NULL ELSE DepDelay END,
  Distance = CASE WHEN Distance = 'NA' THEN NULL ELSE Distance END,
  TaxiIn = CASE WHEN TaxiIn = 'NA' THEN NULL ELSE TaxiIn END,
  TaxiOut = CASE WHEN TaxiOut = 'NA' THEN NULL ELSE TaxiOut END,
  CancellationCode = CASE WHEN CancellationCode = 'NA' THEN NULL ELSE CancellationCode END,
  Diverted = CASE WHEN Diverted = 'NA' THEN NULL ELSE Diverted END,
  CarrierDelay = CASE WHEN CarrierDelay = 'NA' THEN NULL ELSE CarrierDelay END,
  WeatherDelay = CASE WHEN WeatherDelay = 'NA' THEN NULL ELSE WeatherDelay END,
  SecurityDelay = CASE WHEN SecurityDelay = 'NA' THEN NULL ELSE SecurityDelay END,
  LateAircraftDelay = CASE WHEN LateAircraftDelay = 'NA' THEN NULL ELSE LateAircraftDelay END;

-- COMMAND ----------

-- cross checking if the above process has been done 
SELECT *
FROM Bronze_Delta_Clean_Table
WHERE DepTime IS Null ;

-- COMMAND ----------

-- creating one more delta table (with all columns name and data types fixed)
CREATE OR REPLACE TABLE Bronze_Flight_Data_Delta(
Year int,
Month int,
Day int,
DayofWeek int,
Flight_Number int,
Tail_Number string,
Origin_Airport string,
Destination_airport string,
Scheduled_Dept_time float,
Departure_time float,
Departure_Delay int,
Taxi_Out int,
Elapsed_Time int,
Air_Time int,
Distance int,
Taxi_In int,
Scheduled_Arr_time float,
Arrival_time float,
Arrival_Delay int,
Diverted boolean,
Carrier_Delay int,
Cancelled boolean,
Cancellation_Code char(1),
Security_Delay int,
Weather_Delay int,
Late_Aircraft_Delay int
) USING delta Location '/mnt/siribucket/spring2023/project/flight-history-analysis/Bronze/Bronze_Flight_Data_Delta';

-- COMMAND ----------

-- inserting the values from the existing cleaned table to new delta table
INSERT INTO Bronze_Flight_Data_Delta(
Year ,
Month ,
Day ,
DayofWeek ,
Flight_Number ,
Tail_Number ,
Origin_Airport ,
Destination_airport ,
Scheduled_Dept_time ,
Departure_time ,
Departure_Delay ,
Taxi_Out ,
Elapsed_Time ,
Air_Time ,
Distance ,
Taxi_In ,
Scheduled_Arr_time ,
Arrival_time ,
Arrival_Delay ,
Diverted ,
Carrier_Delay,
Cancelled ,
Cancellation_Code ,
Security_Delay ,
Weather_Delay ,
Late_Aircraft_Delay )
SELECT Year,Month,DayofMonth,DayOfWeek,FlightNum,TailNum,Origin,Dest,CRSDepTime,DepTime,DepDelay,TaxiOut,ActualElapsedTime,
AirTime,Distance,TaxiIn,CRSArrTime,ArrTime,ArrDelay,Diverted,CarrierDelay,Cancelled,CancellationCode,SecurityDelay,WeatherDelay,LateAircraftDelay
FROM Bronze_Delta_Clean_Table


-- COMMAND ----------

-- getting rows of new table 
SELECT * FROM Bronze_Flight_Data_Delta
LIMIT 5;

-- COMMAND ----------

-- DESCRIBING THE TABLE
DESC Bronze_Flight_Data_Delta;

-- COMMAND ----------

-- Creating the Final Bronze table for aiport details
CREATE TABLE Bronze_Airports_Details 
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Bronze/Bronze_Delta_Airport_Table'
AS SELECT * FROM Bronze_Airport_Details;

-- COMMAND ----------

-- Creating the Final Bronze table for airline details
CREATE TABLE Bronze_Airline_Details 
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Bronze/Bronze_Delta_Airline_Table'
AS SELECT * FROM Bronze_Airlines_Details;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Silver Table for all the 3 CSV files

-- COMMAND ----------

-- Creating a Silver Table
CREATE TABLE Silver_Flight_Data_Delta
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Silver/Silver_Flight_Data_Delta'
AS SELECT * FROM Bronze_Flight_Data_Delta;

-- COMMAND ----------

SELECT * FROM Silver_Flight_Data_Delta 
LIMIT 5;

-- COMMAND ----------

-- Creating a Silver Table for Airline Details
CREATE TABLE Silver_Airline_Details
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Silver/Silver_Airline_Details'
AS SELECT * FROM Bronze_Airline_Details ;

-- COMMAND ----------

SELECT * FROM Silver_Airline_Details LIMIT 5;

-- COMMAND ----------

-- Creating a Silver Table for Airport Details
CREATE TABLE Silver_Airports_Details
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Silver/Silver_Airports_Details'
AS SELECT * FROM Bronze_Airports_Details;

-- COMMAND ----------

SELECT * FROM Silver_Airports_Details LIMIT 5;

-- COMMAND ----------



-- COMMAND ----------

-- UNIT TESTING 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Installing nutter for unit testing
-- MAGIC %pip install nutter

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Importing Nutter Library
-- MAGIC from runtime.nutterfixture import NutterFixture, tag

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # find the total no of rows in table
-- MAGIC test_tbl = sqlContext.sql("select count(*) from Silver_Flight_Data_Delta")
-- MAGIC count = test_tbl.first()
-- MAGIC print(count)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC class JRTestClass(NutterFixture):
-- MAGIC              
-- MAGIC     def assertion_test1(self):
-- MAGIC         test_tbl1 = sqlContext.sql("select * from Silver_Flight_Data_Delta")
-- MAGIC         first_row = test_tbl1.first()
-- MAGIC         assert (first_row[6] == 'HOU') 
-- MAGIC     
-- MAGIC     def assertion_test2(self):
-- MAGIC         test_tbl2 = sqlContext.sql("select * from Silver_Flight_Data_Delta")
-- MAGIC         second_row = test_tbl2.collect()[1]
-- MAGIC         assert (second_row[0] == 2008)
-- MAGIC
-- MAGIC     def assertion_test3(self):
-- MAGIC         test_tbl3 = sqlContext.sql("select count(*) from Silver_Flight_Data_Delta")
-- MAGIC         count = test_tbl3.first()
-- MAGIC         assert (count[0] > 10000)
-- MAGIC     def assertion_test4(self):
-- MAGIC         test_tbl4 = sqlContext.sql("SELECT * FROM Silver_Flight_Data_Delta LIMIT 1 OFFSET 4")
-- MAGIC         fifth_row = test_tbl4.first()
-- MAGIC         assert(fifth_row[5] == 'N462WN')
-- MAGIC     def assertion_test5(self):
-- MAGIC         test_tbl5 = sqlContext.sql("SELECT * FROM Silver_Flight_Data_Delta LIMIT 1 OFFSET 2")
-- MAGIC         third_row = test_tbl5.first()
-- MAGIC         assert(third_row[14] == 441)
-- MAGIC        

-- COMMAND ----------

-- MAGIC %python
-- MAGIC result = JRTestClass().execute_tests()
-- MAGIC print(result.to_string())

-- COMMAND ----------

-- Using Silver table Create 3 or 4 interesting analysis / Databricks Vizualiation using SQL queries.

-- COMMAND ----------

-- 1. Retrieving the number of flights that were cancelled due to weather conditions in 2008:

SELECT COUNT(*) 
FROM Silver_Flight_Data_Delta
WHERE Cancelled = TRUE AND Cancellation_Code = 'B';

-- COMMAND ----------

-- 2. Retriving the top 10 airports with the highest average delay:

WITH avg_arrival_delay AS (
  SELECT Destination_airport, AVG(Arrival_Delay) AS avg_delay_minutes
  FROM Silver_Flight_Data_Delta
  WHERE Cancelled = FALSE
  GROUP BY Destination_airport
)
SELECT Destination_airport, avg_delay_minutes
FROM avg_arrival_delay
ORDER BY avg_delay_minutes DESC
LIMIT 10;


-- COMMAND ----------



-- COMMAND ----------

-- 3. Lists the top airports with the most delayed flights and their delay counts.

WITH delayed_flights AS (
  SELECT
    year,
    month,
    day,
    origin_airport,
    destination_airport,
    COUNT(*) AS num_delayed_flights
  FROM Silver_Flight_Data_Delta
  WHERE departure_delay > 0
  GROUP BY year, month, day, origin_airport, destination_airport
  ORDER BY num_delayed_flights DESC
  LIMIT 100
)
SELECT *
FROM delayed_flights;


-- COMMAND ----------

-- 4. show all distinct destination airports for flights where the origin airport is 'HOU' and the month is February.

SELECT DISTINCT Destination_airport 
FROM Silver_Flight_Data_Delta 
WHERE Origin_Airport = 'HOU' 
AND Month = 2;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating The Gold Table

-- COMMAND ----------

-- Creating a Gold Table for Flight History CSV File
CREATE TABLE Gold_Flight_Data
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Gold/Gold_Flight_Data'
AS SELECT * FROM Silver_Flight_Data_Delta;

-- COMMAND ----------

select * from Gold_Flight_Data;

-- COMMAND ----------

-- Creating a Gold Table for Flight History CSV File
CREATE TABLE Gold_Airports_Details
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Gold/Gold_Airports_Details'
AS SELECT * FROM Silver_Airports_Details;

-- COMMAND ----------

select * from Gold_Airports_Details;

-- COMMAND ----------

-- Creating a Gold Table for Flight History CSV File
CREATE TABLE Gold_Airline_Details
USING DELTA
LOCATION '/mnt/siribucket/spring2023/project/flight-history-analysis/Gold/Gold_Airline_Details'
AS SELECT * FROM Silver_Airline_Details;

-- COMMAND ----------

select * from Gold_Airline_Details ;

-- COMMAND ----------

DESC Gold_Airports_Details;


-- COMMAND ----------


