CREATE EXTERNAL TABLE spectrum.employee_salaries_staging (
    emp_no INTEGER,
    salary INTEGER,
    from_date DATE,
    to_date DATE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS textfile 
LOCATION 's3://{{params.bucket_name}}/stage/' TABLE PROPERTIES ('skip.header.line.count' = '1');
