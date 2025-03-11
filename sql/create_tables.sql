CREATE TABLE IF NOT EXISTS staging_accounts (
    AccountNumber VARCHAR(20),
    CustomerName VARCHAR(100),
    Balance DECIMAL(18,2),
    Currency VARCHAR(10),
    TransactionDate DATE
);

CREATE TABLE IF NOT EXISTS ods_accounts AS
SELECT 
    AccountNumber,
    UPPER(TRIM(CustomerName)) AS CustomerName,
    Balance,
    Currency,
    CASE 
        WHEN Balance < 1000 THEN 'Low'
        WHEN Balance BETWEEN 1000 AND 10000 THEN 'Medium'
        ELSE 'High'
    END AS CustomerCategory
FROM staging_accounts;

