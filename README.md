# Hardware Store Database Creation
### Overview

This project involves creating a comprehensive database for a hardware store that contains data from monthly invoices spanning from 2016 to 2024. The database facilitates the organization, storage, and retrieval of various hardware store operations and customer transactions.

A Python script was developed to automate the process of loading monthly invoice data into the database. Additionally, static tables containing auxiliary information were preloaded to establish the relationships within the database schema.

## Features

- **Dynamic Data Loading**: The script can process new data files from a to_load folder, insert their contents into the database, and move the processed files to a loaded folder.

- **Seamless Updates**: The system is designed to allow repeated additions of new monthly data without disrupting the existing structure.

- **Error Handling**: The script includes debug lines and mechanisms to catch intentionally erroneous files during the data-loading process, ensuring the database only includes valid data.

- **Comprehensive Schema**: The database schema includes tables for customers, invoices, products, locations, and transactions, ensuring all aspects of the hardware store operations are covered.

## Database Schema

### Static Tables:

1. **tState**
   - `state_id` (Primary Key)
   - `state`

2. **tZip**
   - `zipcode` (Primary Key)
   - `city`
   - `state_id` (Foreign Key to tState)

3. **tCust**
   - `customer_id` (Primary Key)
   - `first`
   - `last`
   - `addr`
   - `zip` (Foreign Key to tZip)

4. **tProd**
   - `product_id` (Primary Key)
   - `product_desc`
   - `unit_price`

### Dynamic Tables:

1. **tInv**
   - `invoice_id` (Primary Key)
   - `customer_id` (Foreign Key to tCust)
   - `day`
   - `month`
   - `year`
   - `time`

2. **tInvDetail**
   - `invoice_id` (Composite Primary Key with prod_id, Foreign Key to tInv)
   - `prod_id` (Composite Primary Key with invoice_id, Foreign Key to tProd)
   - `qty`

## Script Workflow

1. **Loading Data**: Monthly invoice data files are placed in the to_load folder.

2. **Processing**:

    - The script reads the data file and inserts the information into the relevant database tables.

    - Static tables are not altered during this process.

3. **File Management**: Once a file is fully processed, it is moved from the to_load folder to the loaded folder for record-keeping.

4. **Error Handling**:

    - Intentionally erroneous files are tested to ensure that the database rejects them and that errors are logged for debugging purposes.

5. **Repeatability**: This process can be executed as often as needed to include new data.

## Tools and Technologies

- **Programming Language**: Python

- **Database**: SQLite

- **Libraries**: sqlite3, os, pandas, numpy, glob


