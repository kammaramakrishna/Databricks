-- Databricks notebook source
-- MAGIC %md
-- MAGIC SCD Type 1

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS scd1;
USE scd1;

-- Create the source table if it does not exist, including the timestamp column
CREATE TABLE IF NOT EXISTS employee_source (
    Emp_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Salary FLOAT,
    Nationality STRING,
    timestamp TIMESTAMP
);

-- Insert initial data into the source table
INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.0, 'India', CURRENT_TIMESTAMP()), 
    (2, 'John', 'Clair', 2000.0, 'UK', CURRENT_TIMESTAMP());

-- Create the target table if it does not exist
CREATE TABLE IF NOT EXISTS employee_target (
    Emp_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Salary FLOAT,
    Nationality STRING
);

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS deduped_employee_source;

CREATE TEMPORARY view deduped_employee_source AS
SELECT
    es.Emp_ID,
    es.First_Name,
    es.Last_Name,
    es.Salary,
    es.Nationality
FROM employee_source es
JOIN (
    SELECT
        Emp_ID,
        MAX(timestamp) AS max_timestamp
    FROM employee_source
    WHERE timestamp >= DATEADD(minute, -20, CURRENT_TIMESTAMP())
    GROUP BY Emp_ID
) subquery
ON es.Emp_ID = subquery.Emp_ID AND es.timestamp = subquery.max_timestamp;


-- Select data from the temporary deduped table to verify changes
SELECT * FROM deduped_employee_source;


-- COMMAND ----------



-- COMMAND ----------

-- Perform the MERGE operation using the temporary table
MERGE INTO employee_target
USING deduped_employee_source
ON employee_target.Emp_ID = deduped_employee_source.Emp_ID
WHEN MATCHED THEN
  UPDATE SET
    employee_target.First_Name = deduped_employee_source.First_Name,
    employee_target.Last_Name = deduped_employee_source.Last_Name,
    employee_target.Salary = deduped_employee_source.Salary,
    employee_target.Nationality = deduped_employee_source.Nationality
WHEN NOT MATCHED THEN
  INSERT (
    Emp_ID,
    First_Name,
    Last_Name,
    Salary,
    Nationality
  )
  VALUES (
    deduped_employee_source.Emp_ID,
    deduped_employee_source.First_Name,
    deduped_employee_source.Last_Name,
    deduped_employee_source.Salary,
    deduped_employee_source.Nationality
  );

-- Select data from the target table to verify changes
SELECT * FROM employee_target;

-- COMMAND ----------

INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.13, 'USA', CURRENT_TIMESTAMP());

select * from employee_source;

-- COMMAND ----------

drop table employee_source;
drop table employee_target;
drop table deduped_employee_source;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Worked Code

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS scd_pract;
USE scd_pract;

-- Create the source table if it does not exist, including the timestamp column
CREATE TABLE IF NOT EXISTS employee_source (
    Emp_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Salary FLOAT,
    Nationality STRING,
    timestamp TIMESTAMP
);

-- Insert initial data into the source table
INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.0, 'India', CURRENT_TIMESTAMP()), 
    (2, 'John', 'Clair', 2000.0, 'UK', CURRENT_TIMESTAMP());

-- Create the target table if it does not exist
CREATE TABLE IF NOT EXISTS employee_target (
    Emp_ID INT,
    First_Name STRING,
    Last_Name STRING,
    Salary FLOAT,
    Nationality STRING
);

-- Drop the temporary table if it exists
DROP TABLE IF EXISTS deduped_employee_source;


CREATE OR REPLACE TEMPORARY TABLE deduped_employee_source AS
SELECT
    es.Emp_ID,
    es.First_Name,
    es.Last_Name,
    es.Salary,
    es.Nationality
FROM employee_source es
JOIN (
    SELECT
        Emp_ID,
        MAX(timestamp) AS max_timestamp
    FROM employee_source
    WHERE timestamp >= DATEADD(minute, -20, CURRENT_TIMESTAMP())
    GROUP BY Emp_ID
) subquery
ON es.Emp_ID = subquery.Emp_ID AND es.timestamp = subquery.max_timestamp;


-- Select data from the temporary deduped table to verify changes
SELECT * FROM deduped_employee_source;

-- Perform the MERGE operation using the temporary table
MERGE INTO employee_target
USING deduped_employee_source
ON employee_target.Emp_ID = deduped_employee_source.Emp_ID
WHEN MATCHED THEN
  UPDATE SET
    employee_target.First_Name = deduped_employee_source.First_Name,
    employee_target.Last_Name = deduped_employee_source.Last_Name,
    employee_target.Salary = deduped_employee_source.Salary,
    employee_target.Nationality = deduped_employee_source.Nationality
WHEN NOT MATCHED THEN
  INSERT (
    Emp_ID,
    First_Name,
    Last_Name,
    Salary,
    Nationality
  )
  VALUES (
    deduped_employee_source.Emp_ID,
    deduped_employee_source.First_Name,
    deduped_employee_source.Last_Name,
    deduped_employee_source.Salary,
    deduped_employee_source.Nationality
  );

-- Select data from the target table to verify changes
SELECT * FROM employee_target;

select * from deduped_employee_source;


INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.13, 'USA', CURRENT_TIMESTAMP());


select * from employee_source;


drop table employee_source;
drop table employee_target;
drop table deduped_employee_source;
