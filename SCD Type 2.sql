-- Databricks notebook source
create database if not exists scd2;
use scd2;

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
    Nationality STRING,
    start_date DATE,
    end_date DATE,
    current_flag BOOLEAN
);

-- COMMAND ----------



-- COMMAND ----------

-- Create the temporary deduped table
CREATE OR REPLACE TEMPORARY view deduped_employee_source AS
SELECT
    es.Emp_ID,
    es.First_Name,
    es.Last_Name,
    es.Salary,
    es.Nationality,
    es.timestamp
FROM employee_source es
JOIN (
    SELECT
        Emp_ID,
        MAX(timestamp) AS max_timestamp
    FROM employee_source
    GROUP BY Emp_ID
) subquery
ON es.Emp_ID = subquery.Emp_ID AND es.timestamp = subquery.max_timestamp;

-- Select data from the temporary deduped table to verify changes
SELECT * FROM deduped_employee_source;

-- COMMAND ----------



-- COMMAND ----------

-- Perform the SCD Type 2 MERGE operation
MERGE INTO employee_target AS target
USING deduped_employee_source AS source
ON target.Emp_ID = source.Emp_ID AND target.current_flag = TRUE
WHEN MATCHED AND (target.First_Name != source.First_Name OR target.Last_Name != source.Last_Name OR target.Salary != source.Salary OR target.Nationality != source.Nationality) THEN
  UPDATE SET
    target.current_flag = FALSE,
    target.end_date = CURRENT_DATE()
WHEN NOT MATCHED THEN
  INSERT (
    Emp_ID,
    First_Name,
    Last_Name,
    Salary,
    Nationality,
    start_date,
    end_date,
    current_flag
  )
  VALUES (
    source.Emp_ID,
    source.First_Name,
    source.Last_Name,
    source.Salary,
    source.Nationality,
    CURRENT_DATE(),
    NULL,
    TRUE
  );

-- Select data from the target table to verify changes
SELECT * FROM employee_target;

-- COMMAND ----------



-- COMMAND ----------

-- Insert a new record into the source table
INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.13, 'USA', CURRENT_TIMESTAMP());

SELECT * FROM employee_source;

-- COMMAND ----------

drop table employee_source;
drop table employee_target;
drop table deduped_employee_source;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC My Code is not working

-- COMMAND ----------

create database if not exists scd1;
use scd1;

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
    Nationality STRING,
    start_date DATE,
    end_date DATE,
    current_flag BOOLEAN
);

-- Create the temporary deduped table
CREATE OR REPLACE TEMPORARY TABLE deduped_employee_source AS
SELECT
    es.Emp_ID,
    es.First_Name,
    es.Last_Name,
    es.Salary,
    es.Nationality,
    es.timestamp
FROM employee_source es
JOIN (
    SELECT
        Emp_ID,
        MAX(timestamp) AS max_timestamp
    FROM employee_source
    GROUP BY Emp_ID
) subquery
ON es.Emp_ID = subquery.Emp_ID AND es.timestamp = subquery.max_timestamp;

-- Select data from the temporary deduped table to verify changes
SELECT * FROM deduped_employee_source;

-- Perform the SCD Type 2 MERGE operation
MERGE INTO employee_target AS target
USING deduped_employee_source AS source
ON target.Emp_ID = source.Emp_ID AND target.current_flag = TRUE
WHEN MATCHED AND (target.First_Name != source.First_Name OR target.Last_Name != source.Last_Name OR target.Salary != source.Salary OR target.Nationality != source.Nationality) THEN
  UPDATE SET
    target.current_flag = FALSE,
    target.end_date = CURRENT_DATE()
WHEN NOT MATCHED THEN
  INSERT (
    Emp_ID,
    First_Name,
    Last_Name,
    Salary,
    Nationality,
    start_date,
    end_date,
    current_flag
  )
  VALUES (
    source.Emp_ID,
    source.First_Name,
    source.Last_Name,
    source.Salary,
    source.Nationality,
    CURRENT_DATE(),
    NULL,
    TRUE
  );


INSERT INTO employee_target (Emp_ID,First_Name,Last_Name,Salary,Nationality,start_date,end_date,current_flag)
SELECT 
    source.Emp_ID,
    source.First_Name,
    source.Last_Name,
    source.Salary,
    source.Nationality,
    CURRENT_DATE(),
    NULL,
    TRUE
FROM deduped_employee_source source
JOIN employee_target target
ON target.Emp_ID = source.Emp_ID AND target.current_flag = TRUE
WHERE target.First_Name != source.First_Name OR target.Last_Name != source.Last_Name OR target.Salary != source.Salary OR target.Nationality != source.Nationality;

  
-- Select data from the target table to verify changes
SELECT * FROM employee_target;

-- Insert a new record into the source table
INSERT INTO employee_source VALUES
    (1, 'Scott', 'Tiger', 1000.23, 'In', CURRENT_TIMESTAMP());



SELECT * FROM employee_source;

select * from deduped_employee_source;


drop table employee_source;
drop table employee_target;
drop table deduped_employee_source;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Other People Code

-- COMMAND ----------

create DATABASE double;
use database double;


CREATE OR REPLACE TABLE raw_iphone_2 (
    iphone_id INT,
    model STRING,
    price DECIMAL(10, 2),
    date DATE,
    load_date DATE
);

CREATE OR REPLACE TABLE final_iphone_2 (
    iphone_id INT,
    model STRING,
    price DECIMAL(10, 2),
    start_date DATE,
    end_date DATE,
    current_flag BOOLEAN
);

INSERT INTO raw_iphone_2 (iphone_id, model, price, date, load_date) VALUES
(1, 'iPhone 12', 799.99, '2023-01-01', CURRENT_DATE),
(2, 'iPhone 13', 899.99, '2023-02-01', CURRENT_DATE),
(3, 'iPhone 14', 999.99, '2023-03-01', CURRENT_DATE);

MERGE INTO final_iphone_2 AS target
USING (
    SELECT 
        iphone_id,
        model,
        price,
        date AS current_date,
        CURRENT_DATE AS load_date
    FROM raw_iphone_2
) AS source
ON target.iphone_id = source.iphone_id AND target.current_flag = TRUE
WHEN MATCHED AND (
    target.model != source.model OR
    target.price != source.price
) THEN
    -- Update existing records to end date
    UPDATE SET
        target.end_date = source.load_date,
        target.current_flag = FALSE
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (iphone_id, model, price, start_date, end_date, current_flag)
    VALUES (source.iphone_id, source.model, source.price, source.current_date, NULL, TRUE);

    TRUNCATE TABLE raw_iphone_2;

INSERT INTO raw_iphone_2 (iphone_id, model, price, date, load_date) VALUES
(1, 'iPhone 12', 849.99, '2023-01-02', DATEADD(day, 1, CURRENT_DATE)), -- Updated record
(2, 'iPhone 13', 949.99, '2023-02-02', DATEADD(day, 1, CURRENT_DATE)), -- Updated record
(4, 'iPhone 15', 1199.99, '2023-04-01', DATEADD(day, 1, CURRENT_DATE)); -- New record

INSERT INTO raw_iphone_2 (iphone_id, model, price, date, load_date) VALUES
(1, 'iPhone 12', 999.99, '2023-01-02', DATEADD(day, 1, CURRENT_DATE));

CREATE TABLE staging_latest_iphone (
    iphone_id INT,
    model VARCHAR(255),
    price DECIMAL(10, 2),
    load_date DATE
);
INSERT INTO staging_latest_iphone (iphone_id, model, price, load_date)
WITH latest AS (
    SELECT iphone_id, model, price, load_date
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY iphone_id ORDER BY load_date DESC) as rn
        FROM raw_iphone_2
    ) AS subquery
    WHERE rn = 1
)
SELECT iphone_id, model, price, load_date
FROM latest;

MERGE INTO final_iphone_2 AS target
USING (
    SELECT 
        s.iphone_id,
        s.model,
        s.price,
        CURRENT_DATE AS load_date
    FROM staging_latest_iphone s    
) AS source
ON target.iphone_id = source.iphone_id AND target.current_flag = TRUE
WHEN MATCHED AND (
    target.model != source.model OR
    target.price != source.price
) THEN
    -- Update existing records to end date
    UPDATE SET
        target.end_date = source.load_date,
        target.current_flag = FALSE
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (iphone_id, model, price, start_date, end_date, current_flag)
    VALUES (source.iphone_id, source.model, source.price, source.load_date, NULL, TRUE);

-- Insert new records for the latest data not already present in the target table
INSERT INTO final_iphone_2 (iphone_id, model, price, start_date, end_date, current_flag)
SELECT 
    l.iphone_id, 
    l.model, 
    l.price, 
    l.load_date,
    NULL,
    TRUE
FROM staging_latest_iphone l
LEFT JOIN final_iphone_2 t
ON t.iphone_id = l.iphone_id AND t.current_flag = TRUE
WHERE t.iphone_id IS NULL;

SELECT * FROM final_iphone_2;
SELECT * FROM raw_iphone_2;
select * from staging_latest_iphone;

DELETE FROM raw_iphone_2;
DELETE FROM staging_latest_iphone;
DELETE FROM final_iphone_2;


