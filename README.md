# Production ETL pipeline to update large PostgreSQL database

## The Goal
I have a 20 million row postgres database with data about real estate ownership in 4 states; CA, UT, NV, and AZ. Our data supplier sends monthly updates with up to 12 million changes and I have to write a custom ETL script that will successfully ingest the data, standardize new address data through a 3rd-party API, and then update the production database with minimal negative impact to the application users.

# My Approach
## Preparing the raw data file for ingestion
The data is provided in double pipe-delimited format ("||") data files that can be as large as 7 GB. A low-level, performant tool like `sed` will work well to initially transform the data into tab-delimited format which our database can ingest.

`subprocess.run(f"sed 's/||/\t/g' {raw_file} > {raw_file_sed}", shell=True)`

## Creating raw_data table
Use psycopg2 to create the table for our new TSV data.

```
cursor_cs.execute("""
  DROP TABLE if exists delta_raw_adm;
  CREATE TABLE delta_raw_adm
  (admid bigint,
   situsstatecode character varying(2)
   ...
  );
  """)
```


## Loading the raw_data table
Use the postgres COPY command to directly populate the table.


```
with open(raw_file_sed, 'r') as raw_file_in:
  cursor_cs.copy_expert("""
  COPY delta_raw_adm
  FROM STDIN
  (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  QUOTE E'\b',
  ENCODING 'LATIN1',
  FORCE_NOT_NULL (
      ...
      )
  );
  """, raw_file_in)
```

## Preprocess address columns and export for supplier
Our address standardization supplier requires data to be in a particular format so use SQL BTRIM and REGEXP_REPLACE to transform column.

```
cursor_cs.execute("""
  ALTER TABLE delta_raw_adm ADD COLUMN street VARCHAR(256);
  UPDATE delta_raw_adm SET street = BTRIM(REGEXP_REPLACE(CONCAT(
  ContactOwnerMailAddressHouseNumber, ' ',
  ContactOwnerMailAddressStreetDirection, ' ',
  ContactOwnerMailAddressStreetName, ' ',
  ContactOwnerMailAddressStreetSuffix, ' ',
  ContactOwnerMailAddressStreetPostDirection), '\s+', ' ', 'g'));
  ALTER TABLE delta_raw_adm RENAME ContactOwnerMailAddressUnit TO secondary;
  ALTER TABLE delta_raw_adm RENAME ContactOwnerMailAddressCity TO city;
  ALTER TABLE delta_raw_adm RENAME ContactOwnerMailAddressState TO state;
  """)
```

Then, export only the address-specific columns to feed to their API.

```
with connection.cursor('cursor_ss') as cursor_ss:
  cursor_ss.execute("""
      select
      admid,
      street,
      secondary,
      city,
      state
      from delta_raw_adm;
      """)
  header = ['admid', 'street', 'secondary', 'city', 'state']
  with open(ss_in, 'w') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow(header)
    for row in cursor_ss:
        writer.writerow(row)
```

## Join the raw_data and the now-standardized data

```
cursor_cs.execute("""
  create table delta_and_ss as
    select
        ...
    from delta_ss
    join delta_raw_adm on delta_ss.admid = delta_raw_adm.admid;
  """)
```
## Analyzing to update query planner statistics
Calling ANALYZE on a table that has undergone significant, recent change can improve future query performance quite a bit because the query planner will have more accurate statistics about the database.

## Normalize database
The raw data is provided in one large table with no relations and lots of duplication. Our application obviously is using a relational database so we need to fit the data to our database schema by:
* Creating 3 delta relational table schemas
* Populating delta relational tables

## Adding keys and constraints to delta relational tables
Creating table constraints like primary keys and indexes can really speed up query times once tables are populated but if populating a table from scratch, it's much faster to add those constraints after the load has finished like I did here. The reason is because for every INSERT or UPDATE on a row all the rows indexes have to be maintained. If you have many indexes in a table, updating one row means updating one row and updating all of its indexes which can take a long time.

```
cursor_cs.execute("""
  ALTER TABLE delta_jmb_identity
      ADD CONSTRAINT delta_jmb_identity_id_pk PRIMARY KEY (id),
      alter id set not null;

  ALTER TABLE delta_jmb_entity
      ADD CONSTRAINT delta_jmb_entity_id_pk PRIMARY KEY (id),
      ADD CONSTRAINT delta_jmb_entity__identity_id_fk
          FOREIGN KEY (identity_id) REFERENCES delta_jmb_identity on delete cascade,
      alter id set not null,
      alter identity_id set not null;

  ALTER TABLE delta_jmb_parcel
      add constraint delta_jmb_parcel_id_pk PRIMARY KEY (id),
      add constraint delta_jmb_parcel_identity_id_fk
          foreign key (identity_id) references delta_jmb_identity on delete cascade,
      alter identity_id set not null,
      add constraint delta_jmb_parcel_entity_id_fk
          foreign key (entity_id) references delta_jmb_entity on delete cascade,
      alter entity_id set not null;
  Create unique index delta_jmb_parcel_admid_key on delta_jmb_parcel (admid);
  """)
```


## Setting location field for parcel and identity
Use PostGIS extension query to create location column from lat/long.

```
cursor_cs.execute("""
  UPDATE delta_jmb_parcel
  SET parcel_location=st_SetSrid(st_MakePoint(parcel_longitude, parcel_latitude), 4326);

  UPDATE delta_jmb_identity
  SET mail_location=st_SetSrid(st_MakePoint(mail_longitude, mail_latitude), 4326);
  """)
```

## Process the delta and update database
Up until this point I haven't touched any of the production tables. We've only been preparing the new data to execute the updates against.

In order to preserve referential integrity the database needs to be updated in a specific order. Inserting new entries into ancillary tables come first because then the foreign keys are available when updating the main tables. Consequently, the update needs to be done in two complete database passes. This is where all the SQL preprocessing comes in handy. Now that the data is in the correct format I can use python to make the changes to the production database.

* First pass: Inserting new rows into ancillary tables. Then create a set of distinct ids that correspond to the main table's entries to be updated
* Second pass: Update or insert into main table

## Detecting and deleting orphaned rows
After update is complete, detect and delete rows that no longer have references.

```
cursor_cs.execute('''
  delete from jmb_entity where id in (
  select jmb_entity.id
  from
      jmb_entity
      left join jmb_parcel on jmb_parcel.entity_id = jmb_entity.id
  where jmb_parcel.id is null);
  ''')
```

## Commit changes
Commit changes to the database.

`connection.commit()`

# Conclusion
This project included architecting an application that solves a real-world data pipeline problem. This script automated the ingestion, cleaning, and updating of a large dataset, as well as automated many database administration tasks required to tune the Postgres server for performance and the results were robust and consistent.
