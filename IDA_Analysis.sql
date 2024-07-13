create database IDA_loans;
use IDA_loans;


create table India(
End_of_Period  text,
Credit_Number text,
region text,
country_code text,
country text,
borrower text,
credit_status text,
service_charge_rate numeric,
currency_of_commitment text,
project_id text,
project_name text,
original_principal_amount numeric,
cancelled_amount numeric,
undisbursed_amount numeric,
disbursed_amount numeric, 
repaid_to_ida numeric,
due_to_ida numeric,
exchange_adjustment text,
borrower_s_obligation numeric,
sold_3rd_party numeric,
repaid_3rd_party numeric,
due_3rd_party numeric,
Credits_held numeric,
first_repayment_date text,
last_repayment_date text,
agreement_signing_date text,
board_approval_date text,
effective_date_most_recent text,
closed_date_most_recent text,
last_disbursement_date text
);
create table south_asia(End_of_Period  text,
Credit_Number text,
region text,
country_code text,
country text,
borrower text,
credit_status text,
service_charge_rate numeric,
currency_of_commitment text,
project_id text,
project_name text,
original_principal_amount numeric,
cancelled_amount numeric,
undisbursed_amount numeric,
disbursed_amount numeric, 
repaid_to_ida numeric,
due_to_ida numeric,
exchange_adjustment text,
borrower_s_obligation numeric,
sold_3rd_party numeric,
repaid_3rd_party numeric,
due_3rd_party numeric,
Credits_held numeric,
first_repayment_date text,
last_repayment_date text,
agreement_signing_date text,
board_approval_date text,
effective_date_most_recent text,
closed_date_most_recent text,
last_disbursement_date text
);

SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE "C:/Users/Raghav/Desktop/SQL Project/Cleaned_IDA_Statement_Of_Credits_and_Grants.csv"
INTO TABLE India
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "C:/Users/Raghav/Desktop/SQL Project/IDA_Statement_Of_Credits_and_Grants_-_Historical_Data_20240628.csv"
INTO TABLE South_asia
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

WITH LatestRecords AS (
    SELECT
        Country,
        Credit_Number,
        Original_Principal_Amount,
        Repaid_to_IDA,
        CASE
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 1960 AND 1969 THEN '1960-1970'
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 1970 AND 1979 THEN '1970-1980'
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 1980 AND 1989 THEN '1980-1990'
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 1990 AND 1999 THEN '1990-2000'
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 2000 AND 2009 THEN '2000-2010'
            WHEN CAST(SUBSTR(Effective_Date_Most_Recent, 7, 4) AS SIGNED) BETWEEN 2010 AND 2019 THEN '2010-2020'
            ELSE 'Other'
        END AS Decade,
        ROW_NUMBER() OVER (PARTITION BY Country, Credit_Number ORDER BY CAST(SUBSTR(end_of_period, 7, 4) AS SIGNED) DESC) AS rn
    FROM
        south_asia
)
SELECT
    Country,
    Decade,
    COUNT(DISTINCT Credit_Number) AS Unique_Loans
FROM
    LatestRecords
WHERE
    rn = 1 AND Decade != 'Other'
GROUP BY
    Country,
    Decade
ORDER BY
    Country,
    Decade;

-- How many credits or grants has India accessed?
SELECT 
    COUNT(DISTINCT Credit_Number) AS total_unique_credits_or_grants
FROM 
    India
WHERE 
    country = 'India';

-- Top 5 Porjects funded using IDA credits --
-- Subquery to get the most recent entry for each credit number within each project
WITH LatestCreditData AS (
    SELECT 
        project_name,
        project_id,
        Credit_number,
        original_principal_amount,
        cancelled_amount,
        ROW_NUMBER() OVER (PARTITION BY project_id, Credit_number ORDER BY SUBSTRING(end_of_period, 1, 10) DESC) as rn
    FROM India
    WHERE country = 'India'
)

-- Filter to include only the latest entry for each credit number
, FilteredCreditData AS (
    SELECT 
        project_name,
        project_id,
        Credit_number,
        original_principal_amount,
        cancelled_amount
    FROM LatestCreditData
    WHERE rn = 1
)
-- Main query to sum the funding for each project and get the top 10 projects
SELECT 
    project_name, 
    project_id,
    SUM(original_principal_amount - cancelled_amount) AS total_funding
FROM FilteredCreditData
GROUP BY project_name, project_id
ORDER BY total_funding DESC
LIMIT 10;

-- Total amount owed to IDA --
-- Subquery to get the most recent entry for each credit number
WITH LatestCreditStatus AS (
    SELECT 
        Credit_number, 
        due_to_ida,
        ROW_NUMBER() OVER (PARTITION BY Credit_number ORDER BY SUBSTRING(end_of_period, 1, 10) DESC) as rn
    FROM India
    WHERE country = 'India'
)

-- Filter to include only the latest entry for each credit number
, FilteredCreditStatus AS (
    SELECT 
        Credit_number,
        due_to_ida
    FROM LatestCreditStatus
    WHERE rn = 1
)

-- Main query to sum the total amount owed to IDA
SELECT 
    SUM(due_to_ida) AS total_amount_owed
FROM FilteredCreditStatus;


-- Breakdown of loan status --
-- Subquery to get the latest status for each loan --
WITH LatestLoanStatus AS (
    SELECT 
        Credit_number,
        credit_status,
        ROW_NUMBER() OVER (PARTITION BY Credit_number ORDER BY end_of_period DESC) as rn
    FROM India
    WHERE country = 'India'
)

-- Main query to categorize loan statuses
SELECT    
    CASE
        WHEN credit_status = 'Disbursing' OR credit_status = 'Disbursed' OR credit_status = 'Disbursing&Repaying' OR credit_status = 'Fully Disbursed' or credit_status='Repaying' THEN 'Outstanding'
        WHEN credit_status = 'Fully Repaid' OR credit_status = 'Repaid' THEN 'Settled'
        WHEN credit_status = 'Cancelled' OR credit_status = 'Fully Cancelled' THEN 'Cancelled'
        ELSE 'In Process'
    END AS loan_status,
    COUNT(*) AS count
FROM LatestLoanStatus
WHERE rn = 1
GROUP BY loan_status;


-- Decade wise loan top 3 projects
-- Subquery to get the most recent entry for each credit number within each project
WITH LatestCreditData AS (
    SELECT 
		cancelled_amount,
        project_id,
        project_name,
        Credit_number,
        original_principal_amount,
        end_of_period,
        effective_date_most_recent,
        ROW_NUMBER() OVER (PARTITION BY project_id, Credit_number ORDER BY SUBSTRING(end_of_period, 1, 10) DESC) as rn
    FROM India
    WHERE country = 'India'
)

-- Filter to include only the latest entry for each credit number
, FilteredCreditData AS (
    SELECT 
		cancelled_amount,
        project_id,
        project_name,
        Credit_number,
        original_principal_amount,
        effective_date_most_recent
    FROM LatestCreditData
    WHERE rn = 1
)

-- Subquery to classify projects by decade and sum the total credits taken
, ProjectDecades AS (
    SELECT
        project_id,
        project_name,
        SUM(original_principal_amount-cancelled_amount) AS total_funding,
        CASE 
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 1960 AND 1969 THEN '1960-1970'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 1970 AND 1979 THEN '1970-1980'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 1980 AND 1989 THEN '1980-1990'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 1990 AND 1999 THEN '1990-2000'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 2000 AND 2009 THEN '2000-2010'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 2010 AND 2019 THEN '2010-2020'
            WHEN CAST(SUBSTR(effective_date_most_recent, 7, 10) AS SIGNED) BETWEEN 2020 AND 2029 THEN '2020-2030'
            ELSE 'Unknown'
        END AS decade
    FROM
        FilteredCreditData
    WHERE
        effective_date_most_recent IS NOT NULL
    GROUP BY
        project_id, project_name, decade
)

-- Final query to get the top investment projects of each decade
SELECT
    decade,
    project_id,
    project_name,
    total_funding
FROM (
    SELECT
        decade,
        project_id,
        project_name,
        total_funding,
        ROW_NUMBER() OVER (PARTITION BY decade ORDER BY total_funding DESC) AS my_rank
    FROM
        ProjectDecades
) ranked_projects
WHERE
    my_rank <= 3
ORDER BY
    decade,
    my_rank;


-- Disburse Rate
WITH MostRecentLoans AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Credit_Number ORDER BY STR_TO_DATE(End_of_Period, '%Y-%m-%d %H:%i:%s') DESC) AS rn
    FROM
        India
)
SELECT 
    SUM(Disbursed_Amount) / SUM(Original_Principal_amount) * 100 AS Disburse_rate
FROM 
    india
WHERE 
    country = 'India';
--
WITH MostRecentLoans AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Credit_Number ORDER BY STR_TO_DATE(End_of_Period, '%Y-%m-%d %H:%i:%s') DESC) AS rn
    FROM
        India
)
SELECT
    Year,
    SUM(TotalAmountTaken) AS TotalAmountTaken,
    SUM(TotalAmountRepaid) AS TotalAmountRepaid
FROM (
    SELECT
        SUBSTR(effective_date_most_recent, 7, 4) AS Year,
        original_principal_amount- cancelled_amount AS TotalAmountTaken,
        0 AS TotalAmountRepaid
    FROM
        MostRecentLoans
    WHERE
        rn = 1
    AND
        CAST(SUBSTR(effective_date_most_recent, 7, 4) AS UNSIGNED) BETWEEN 2011 AND 2021

    UNION ALL

    SELECT
        SUBSTR(last_repayment_date, 7, 4) AS Year,
        0 AS TotalAmountTaken,
        original_principal_amount-cancelled_amount AS TotalAmountRepaid
    FROM
        MostRecentLoans
    WHERE
        rn = 1
    AND
        CAST(SUBSTR(last_repayment_date, 7, 4) AS UNSIGNED) BETWEEN 2011 AND 2021
) AS combined
GROUP BY
    Year
ORDER BY
    Year;

WITH LatestCreditData AS (
    SELECT 
		due_to_IDA,
        Credit_number,
        end_of_period,
        effective_date_most_recent,
        ROW_NUMBER() OVER (PARTITION BY project_id, Credit_number ORDER BY SUBSTRING(end_of_period, 1, 10) DESC) as rn
    FROM India
    WHERE country = 'India'
)
Select
sum(due_to_IDA) as TotalAmountDue
from LatestCreditData
where
rn=1;

select count(distinct credit_number) from India
where country ='India';