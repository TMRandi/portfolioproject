CREATE TABLE data_science_job_stagging
like data_science_job;

SELECT *
FROM data_science_job_stagging;

INSERT data_science_job_stagging
SELECT *
FROM data_science_job;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY work_year, job_title, job_category, salary_currency, salary, salary_in_usd, employee_residence, 
experience_level, employment_type, work_setting, company_location, company_size) AS row_num
FROM data_science_job_stagging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY work_year, job_title, job_category, salary_currency, salary, salary_in_usd, employee_residence, 
experience_level, employment_type, work_setting, company_location, company_size) AS row_num
FROM data_science_job_stagging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `data_science_job_stagging2` (
  `work_year` int DEFAULT NULL,
  `job_title` text,
  `job_category` text,
  `salary_currency` text,
  `salary` int DEFAULT NULL,
  `salary_in_usd` int DEFAULT NULL,
  `employee_residence` text,
  `experience_level` text,
  `employment_type` text,
  `work_setting` text,
  `company_location` text,
  `company_size` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM data_science_job_stagging2
WHERE row_num > 1;

INSERT INTO data_science_job_stagging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY work_year, job_title, job_category, salary_currency, salary, salary_in_usd, employee_residence, 
experience_level, employment_type, work_setting, company_location, company_size) AS row_num
FROM data_science_job_stagging;


DELETE
FROM data_science_job_stagging2
WHERE row_num > 1;

SELECT *
FROM data_science_job_stagging2;


SELECT distinct job_title
FROM data_science_job_stagging2;

update data_science_job_stagging2
set job_title = 'Data Analyst'
WHERE job_title LIKE 'Data Analyst%';

update data_science_job_stagging2
set job_title = 'Data Engineer'
WHERE job_title LIKE 'Data Engineer%';

update data_science_job_stagging2
set job_title = 'Data Scientist'
WHERE job_title LIKE 'Data Scientist%';

update data_science_job_stagging2
set job_title = 'Machine Learning Engineer'
WHERE job_title LIKE 'Machine Learning Engineer%';

update data_science_job_stagging2
set job_title = 'Statistician'
WHERE job_title LIKE 'Statistician%';

select *
FROM data_science_job_stagging2
WHERE job_category IS NULL
OR job_category = '';

delete
FROM data_science_job_stagging2
WHERE job_category IS NULL
OR job_category = '';

select *
FROM data_science_job_stagging2;




