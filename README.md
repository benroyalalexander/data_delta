# Custom production ETL pipeline to update large PostgreSQL database

## The Goal
I have a 20 million row postgres database with data about real estate ownership in 4 states; CA, UT, NV, and AZ. Our data supplier sends monthly updates with up to 12 million changes and I have to write a custom ETL script that will successfully ingest the data, standardize new address data through a 3rd-party API, and then update the production database with minimal negative impact to the application users.

# My Approach
## Preparing the raw data file for ingestion
The data is provided in double pipe-delimited format ("||") data files that can be as large as 7 GB. A low-level, performant tool like `sed` will work well to initially transform the data into tab-delimited format which our database can ingest.

## Creating raw_data table
Use psycopg2 to create the table for our new TSV data
## Loading the raw_data table
Use the postgres COPY command to directly populate the table
## Preprocess address columns and export for supplier
Our address standardization supplier requires data to be in a particular format so use SQL BTRIM and REGEXP_REPLACE to transform column. Then, export only the address-specific columns to get feed to their API.
## Define another database table to ingest now-standardized addresses 
## Join the raw_data and the now-standardized data
## Analyzing tables to update query planner statistics
Calling ANALYZE on a table that has undergone significant, recent change can improve future query performance quite a bit becaue the query planner will have more accurate statistics about the database.
## Normalize database
The raw data is provided in one large table with no relations and lots of duplication. Our application obviously is using a relational database so we need to fit the data to our database schema by:
* Creating 3 delta relational table schemas
* Populating delta relational tables
## Adding keys and constraints to delta relational tables
Creating table constraints like primary keys and indexes can really speed up query times once tables are populated but if populating a table from scratch it's much faster to add those constraints after the load has finished like I did here. The reason is because for every INSERT or UPDATE on a row all the rows indexes have to be maintained. If you have many indexes in a table, updating one row means updating one row and updating all of its indexes which can take a long time.
## Setting location field for parcel and identity
Use PostGIS extension query to create location column from lat/long
## Process the delta and update database
In order to preserve referential integrity the database needs to be updated in a specific order. Inserting new entries into ancillary tables come first because then the foreign keys are available when updating the main tables. Consequently, the update needs to be done in 3 steps and two complete database passes.

* First pass: Inserting new rows into ancillary tables
* Then create a set of distinct ids that correspond to the main tables entries to be updated
* Second pass: Update or insert into main table

## Detecting and deleting orphaned rows
After update is complete, detect and delete rows that no longer have references
## Commit changes
Commit changes to the database
## Dropping all temporary working tables
Cleanup the working tables in Postgres
## Delete working files on EC2 instance running ETL script
Cleanup working files

# Conclusion
This project included architecting an application that solves a real-world data pipeline problem. This script automated the ingestion, cleaning, and updating of a large dataset as well as automated many database administration tasks required to tune the Postgres server for performance. The results were robust and consistent.
