import db_connection as dbc
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql
from fuzzy_matching import print_now

start = time.time()
rental_flag_query = '''
SELECT array_agg(admid)
FROM jmb_parcel
    join jmb_identity on jmb_parcel.identity_id = jmb_identity.id
WHERE land_use in (
    '181',
    '366',
    '369',
    '375',
    '378',
    '380',
    '385',
    '386',
    '388')
and not (owner_occupied is true) 
AND NOT mail_address ilike '%PO BOX%'
--Add address valid check
AND rdi = 'Residential'
GROUP BY jmb_identity.id
HAVING count(admid) > 1
ORDER BY count(admid) DESC;'''

rental_count_query = '''
    update jmb_identity 
    set rental_count = sub.cnt 
    from (
    select identity_id, 
            count(*) as cnt
    from jmb_parcel 
    where rental_flag = TRUE 
    group by jmb_parcel.identity_id) as sub 
    where jmb_identity.id = sub.identity_id
;'''

with psycopg2.connect(dbname=dbc.dbname,
                      host=dbc.host,
                      user=dbc.user,
                      password=dbc.password) as connection:
    with connection.cursor() as cursor:
        print_now('Starting rental flag query')
        cursor.execute(rental_flag_query)
        data = cursor.fetchall()
        rental_ids = []
        for i, row in enumerate(data):
            # if i > 1000:
            #     break
            for parcel in row[0]:
                rental_ids.append(parcel)
        if rental_ids:
            print(f'# of single family rental ids: {len(rental_ids)}')
            print_now('Dropping rental_flag column and adding again')
            cursor.execute('''alter table jmb_parcel drop rental_flag;''')
            cursor.execute('''alter table jmb_parcel add column rental_flag boolean;''')
            # values = psycopg2.sql.SQL(',  '.join(f'({x})' for x in rental_ids))
            # query = psycopg2.sql.SQL('''
            # UPDATE jmb_parcel
            # SET rental_flag = TRUE
            # WHERE admid in (values {0});'''.format(values.as_string(cursor)))
            cursor.execute('''
            create temporary table vals (
            admid integer
            )
            ''')
            values = psycopg2.sql.SQL(',  '.join(f'({x})' for x in rental_ids))
            query = psycopg2.sql.SQL('''
            Insert into vals
            values {0};'''.format(values.as_string(cursor)))
            cursor.execute(query)
            print_now('Doing update for rental flag algorithm...')
            cursor.execute('''
            UPDATE jmb_parcel
            SET rental_flag = TRUE
            FROM
                vals
            WHERE
                jmb_parcel.admid = vals.admid;
            ''')
            #print_now('Updating rental_flag to True where last_vacant is not null')
            #cursor.execute('''
            #Update jmb_parcel
            #set rental_flag = True
            #where last_vacant is not null
            #and last_vacant >= current_date - interval '180' day;
            #''')
        else:
            print('No rental ids, exiting now.')
            exit()
        print_now('Dropping rental_count column and adding again')
        cursor.execute('''alter table jmb_identity drop rental_count;''')
        cursor.execute('''alter table jmb_identity add column rental_count smallint;''')
        print_now('Setting the rental_count column values')
        cursor.execute(rental_count_query)
print_now('Total run time')
