<h1 align="left">Snoop</h1>

This repository is used to transform and upload transactional data into PostgreSQL.

Structure of zipped JSON files:

| Field           | Description                                                                                                                          |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| customerId      | Customer Identifier                                                                                                                  |
| customerName    | First name and Surname of the customer                                                                                               |
| transactionDate | Date of Transaction                                                                                                                  |
| transactionId   | Unique transaction ID                                                                                                                |
| sourceDate      | The timestamp of when the record was created in the source system                                                                    |
| merchantId      | Merchant Identifier                                                                                                                  |
| categoryId      | Category Identifier                                                                                                                  |
| amount          | Amount of transaction                                                                                                                |
| description     | The description of the transaction. This is typically “Merchant Name | Category Name” but can be overridden by customers (Free Text) |
| currency        | The currency code related to the transaction. The only valid values for this are EUR, GBP and USD.                                   |

The following data quality checks are applied before uploading to relevant tables:
1. Valid currency (GBP, USD, EUR)
2. Valid transaction dates
3. Duplicate records

<h2 align="left">Getting Started</h2>

### PostgreSQL
To connect to your local environment update the parameters in the [connection](psql_conn.py) file:
- host
- database
- user
- password

Create the following tables:
```
CREATE TABLE customers (
    customerId VARCHAR(36) PRIMARY KEY,
    transactionDate DATE
);


CREATE TABLE transactions (
    customerId VARCHAR(36),
    transactionId VARCHAR(36) PRIMARY KEY,
    transactionDate DATE,
    sourceDate DATE,
    merchantId INT,
    categoryId INT,
    currency VARCHAR(3),
    amount DECIMAL(10, 2),
    description TEXT
);

CREATE TABLE error_logs (
    customerId VARCHAR(36),
    transactionId VARCHAR(36) NULL,  -- Allow NULL values
    transactionDate DATE,
    sourceDate DATE,
    merchantId INT,
    categoryId INT,
    currency VARCHAR(3),
    amount DECIMAL(10, 2),
    description TEXT,
    data_quality_check VARCHAR(36)
);
```

For more info on installation, visit [PostgreSQL](https://www.postgresql.org/docs/current/tutorial-install.html).
