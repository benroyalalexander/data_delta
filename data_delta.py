"""
Load and process the delta then delete parcels, working files and tables

This script loads the raw adm delta file and preps db for
delta_process.py then processes the adm data delta tables and updates
our database. It then processes adm delete file, deletes parcels, sets
inactive identities.

DUP_TAX_ASSESSOR_**** and DUP_PROPERTYDELETES_**** need to be in
the same directory as this script or you'll need to rewrite the file
path below for raw_file and delete_file.

Requires the "inactive" field on jmb_identity.
"""

import csv
import db_connection as dbc
import psycopg2
from psycopg2.extras import NamedTupleCursor
import subprocess
import time
import fuzzy_matching
import sys


start = time.time()
# delta_num = input('delta_num: ')
print('Args (y for yes): python script_name delta_num skip_ss skip_load_process')
delta_num = sys.argv[1]
# skip_ss = True if input('Skip ss?: ("y" for yes) ') == 'y' else False
# skip_load_process = True if input('Skip ss?: ("y" for yes) ') == 'y' else False
try:
    skip_ss = True if sys.argv[2] == 'y' else False
except IndexError:
    skip_ss = False
try:
    skip_load_process = True if sys.argv[3] == 'y' else False
except IndexError:
    skip_load_process = False
raw_file = f'DUP_TAXASSESSOR_{delta_num}.txt'
raw_file_sed = f'DUP_TAXASSESSOR_{delta_num}.tsv'
ss_in = f'ss_in_{delta_num}.tsv'
ss_out = f'ss_out_{delta_num}.tsv'
relationships = dict()
delete_file = f'DUP_PROPERTYDELETES_{delta_num}.txt'

fuzzy_matching.print_now()
with psycopg2.connect(dbname=dbc.dbname,
                      host=dbc.host,
                      user=dbc.user,
                      password=dbc.password,
                      cursor_factory=NamedTupleCursor) as connection, \
connection.cursor('cursor_ss', withhold=True) as cursor_ss, \
connection.cursor() as cursor_cs:
    """
    Explicitly set the server-side cursor to scrollable so it can be
    reset and iterated over twice.
    """
    if not skip_load_process:
        cursor_ss.scrollable = True
        # Prepare the raw data file for ingestion
        fuzzy_matching.print_now('Preparing the raw data file for ingestion')
        subprocess.check_call(f"sed 's/||/\t/g' {raw_file} > {raw_file_sed}", shell=True)

        # Create delta_raw_adm table
        fuzzy_matching.print_now('Creating delta_raw_adm table')
        cursor_cs.execute("""
        DROP TABLE if exists delta_raw_adm;
        CREATE TABLE delta_raw_adm
        (
         admid bigint,
         situsstatecode character varying(2),
         situscounty character varying(50),
         propertyjurisdictionname character varying(50),
         situsstatecountyfips character(5),
         combinedstatisticalarea character varying(100),
         cbsaname character varying(100),
         cbsacode character(5),
         msaname character varying(100),
         msacode character(5),
         metropolitandivision character varying(100),
         minorcivildivisionname character varying(100),
         minorcivildivisioncode character(5),
         neighborhoodcode character varying(10),
         censusfipsplacecode character(5),
         censustract integer,
         censusblockgroup smallint,
         censusblock integer,
         parcelnumberraw character varying(60),
         parcelnumberformatted character varying(60),
         parcelnumberyearadded smallint,
         parcelnumberalternate character varying(60),
         parcelmapbook character varying(10),
         parcelmappage character varying(10),
         parcelnumberyearchange smallint,
         parcelnumberprevious character varying(60),
         parcelaccountnumber character varying(35),
         propertyaddressfull character varying(150),
         propertyaddresshousenumber character varying(25),
         propertyaddressstreetdirection character varying(10),
         propertyaddressstreetname character varying(100),
         propertyaddressstreetsuffix character varying(25),
         propertyaddressstreetpostdirection character varying(10),
         propertyaddressunitprefix character varying(20),
         propertyaddressunitvalue character varying(25),
         propertyaddresscity character varying(50),
         propertyaddressstate character varying(2),
         propertyaddresszip character varying(5),
         propertyaddresszip4 character varying(4),
         propertyaddresscrrt character varying(4),
         propertyaddressinfoprivacy boolean,
         congressionaldistricthouse smallint,
         propertylatitude numeric(10,6),
         propertylongitude numeric(10,6),
         geoquality character varying(20),
         legaldescription character varying(255),
         legalrange character varying(35),
         legaltownship character varying(35),
         legalsection character varying(35),
         legalquarter character varying(4),
         legalquarterquarter character varying(4),
         legalsubdivision character varying(50),
         legalphase character varying(10),
         legaltractnumber character varying(10),
         legalblock1 character varying(10),
         legalblock2 character varying(10),
         legallotnumber1 character varying(10),
         legallotnumber2 character varying(10),
         legallotnumber3 character varying(10),
         legalunit character varying(10),
         partyowner1namefull character varying(150),
         partyowner1namefirst character varying(50),
         partyowner1namemiddle character varying(20),
         partyowner1namelast character varying(50),
         partyowner1namesuffix character varying(20),
         trustdescription character varying(50),
         companyflag boolean,
         partyowner2namefull character varying(150),
         partyowner2namefirst character varying(50),
         partyowner2namemiddle character varying(20),
         partyowner2namelast character varying(50),
         partyowner2namesuffix character varying(20),
         ownertypedescription1 character varying(50),
         ownershipvestingrelationcode character varying(3),
         partyowner3namefull character varying(150),
         partyowner3namefirst character varying(50),
         partyowner3namemiddle character varying(20),
         partyowner3namelast character varying(50),
         partyowner3namesuffix character varying(20),
         partyowner4namefull character varying(150),
         partyowner4namefirst character varying(50),
         partyowner4namemiddle character varying(20),
         partyowner4namelast character varying(50),
         partyowner4namesuffix character varying(20),
         ownertypedescription2 character varying(50),
         contactownermailingcounty character varying(50),
         contactownermailingfips character(5),
         contactownermailaddressfull character varying(100),
         contactownermailaddresshousenumber character varying(20),
         contactownermailaddressstreetdirection character varying(10),
         contactownermailaddressstreetname character varying(100),
         contactownermailaddressstreetsuffix character varying(25),
         contactownermailaddressstreetpostdirection character varying(10),
         contactownermailaddressunitprefix character varying(20),
         contactownermailaddressunit character varying(20),
         contactownermailaddresscity character varying(50),
         contactownermailaddressstate character(2),
         contactownermailaddresszip character(5),
         contactownermailaddresszip4 character(4),
         contactownermailaddresscrrt character varying(4),
         contactownermailaddressinfoformat character varying(50),
         contactownermailinfoprivacy boolean,
         statusowneroccupiedflag boolean,
         deedowner1namefull character varying(150),
         deedowner1namefirst character varying(50),
         deedowner1namemiddle character varying(20),
         deedowner1namelast character varying(50),
         deedowner1namesuffix character varying(20),
         deedowner2namefull character varying(150),
         deedowner2namefirst character varying(50),
         deedowner2namemiddle character varying(20),
         deedowner2namelast character varying(50),
         deedowner2namesuffix character varying(20),
         deedowner3namefull character varying(150),
         deedowner3namefirst character varying(50),
         deedowner3namemiddle character varying(20),
         deedowner3namelast character varying(50),
         deedowner3namesuffix character varying(20),
         deedowner4namefull character varying(150),
         deedowner4namefirst character varying(50),
         deedowner4namemiddle character varying(20),
         deedowner4namelast character varying(50),
         deedowner4namesuffix character varying(20),
         taxyearassessed smallint,
         taxassessedvaluetotal bigint,
         taxassessedvalueimprovements bigint,
         taxassessedvalueland bigint,
         taxassessedimprovementsperc numeric(9,2),
         previousassessedvalue bigint,
         taxmarketvalueyear smallint,
         taxmarketvaluetotal bigint,
         taxmarketvalueimprovements bigint,
         taxmarketvalueland bigint,
         taxmarketimprovementsperc numeric(6,2),
         taxfiscalyear smallint,
         taxratearea character varying(20),
         taxbilledamount numeric(18,2),
         taxdelinquentyear smallint,
         lastassessortaxrollupdate date,
         assrlastupdated date,
         taxexemptionhomeownerflag boolean,
         taxexemptiondisabledflag boolean,
         taxexemptionseniorflag boolean,
         taxexemptionveteranflag boolean,
         taxexemptionwidowflag boolean,
         taxexemptionadditional character varying(10),
         yearbuilt smallint,
         yearbuilteffective smallint,
         zonedcodelocal character varying(50),
         propertyusemuni character varying(10),
         propertyusegroup character varying(50),
         propertyusestandardized character varying(4),
         assessorlastsaledate date,
         assessorlastsaleamount bigint,
         assessorpriorsaledate date,
         assessorpriorsaleamount bigint,
         lastownershiptransferdate date,
         lastownershiptransferdocumentnumber character varying(20),
         lastownershiptransfertransactionid bigint,
         deedlastsaledocumentbook character varying(15),
         deedlastsaledocumentpage character varying(15),
         deedlastdocumentnumber character varying(25),
         deedlastsaledate date,
         deedlastsaleprice bigint,
         deedlastsaletransactionid bigint,
         areabuilding integer,
         areabuildingdefinitioncode character varying(2),
         areagross integer,
         area1stfloor integer,
         area2ndfloor integer,
         areaupperfloors integer,
         arealotacres numeric(18,7),
         arealotsf numeric(18,7),
         arealotdepth numeric(9,1),
         arealotwidth numeric(9,1),
         roomsatticarea integer,
         roomsatticflag boolean,
         roomsbasementarea integer,
         roomsbasementareafinished integer,
         roomsbasementareaunfinished integer,
         parkinggarage character varying(3),
         parkinggaragearea integer,
         parkingcarport character varying(3),
         parkingcarportarea bigint,
         hvaccoolingdetail character varying(3),
         hvacheatingdetail character varying(3),
         hvacheatingfuel character varying(3),
         utilitiessewageusage character varying(3),
         utilitieswatersource character varying(3),
         utilitiesmobilehomehookupflag boolean,
         foundation character varying(3),
         construction character varying(3),
         interiorstructure character varying(3),
         plumbingfixturescount smallint,
         constructionfireresistanceclass character varying(3),
         safetyfiresprinklersflag boolean,
         flooringmaterialprimary smallint,
         bathcount numeric(7,3),
         bathpartialcount smallint,
         bedroomscount smallint,
         roomscount integer,
         storiescount smallint,
         unitscount integer,
         roomsbonusroomflag boolean,
         roomsbreakfastnookflag boolean,
         roomscellarflag boolean,
         roomscellarwineflag boolean,
         roomsexerciseflag boolean,
         roomsfamilycode character varying(4),
         roomsgameflag boolean,
         roomsgreatflag boolean,
         roomshobbyflag boolean,
         roomslaundryflag boolean,
         roomsmediaflag boolean,
         roomsmudflag boolean,
         roomsofficearea integer,
         roomsofficeflag boolean,
         roomssaferoomflag boolean,
         roomssittingflag boolean,
         roomsstormshelter boolean,
         roomsstudyflag boolean,
         roomssunroomflag boolean,
         roomsutilityarea integer,
         roomsutilitycode character varying(4),
         fireplace character varying(2),
         fireplacecount smallint,
         accessabilityelevatorflag boolean,
         accessabilityhandicapflag boolean,
         escalatorflag boolean,
         centralvacuumflag boolean,
         contentintercomflag boolean,
         contentsoundsystemflag boolean,
         wetbarflag boolean,
         securityalarmflag boolean,
         structurestyle smallint,
         exterior1code character varying(3),
         roofmaterial character varying(3),
         roofconstruction character varying(3),
         contentstormshutterflag boolean,
         contentoverheaddoorflag boolean,
         viewdescription character varying(3),
         porchcode character varying(3),
         porcharea integer,
         patioarea integer,
         deckflag boolean,
         deckarea integer,
         featurebalconyflag boolean,
         balconyarea integer,
         breezewayflag boolean,
         parkingrvparkingflag boolean,
         parkingspacecount integer,
         drivewayarea integer,
         drivewaymaterial character varying(2),
         pool smallint,
         poolarea integer,
         contentsaunaflag boolean,
         topographycode smallint,
         fencecode character varying(2),
         fencearea integer,
         courtyardflag boolean,
         courtyardarea integer,
         arborpergolaflag boolean,
         sprinklersflag boolean,
         golfcoursegreenflag boolean,
         tenniscourtflag boolean,
         sportscourtflag boolean,
         arenaflag boolean,
         waterfeatureflag boolean,
         pondflag boolean,
         boatliftflag boolean,
         buildingscount smallint,
         bathhousearea integer,
         bathhouseflag boolean,
         boataccessflag boolean,
         boathousearea integer,
         boathouseflag boolean,
         cabinarea integer,
         cabinflag boolean,
         canopyarea integer,
         canopyflag boolean,
         gazeboarea integer,
         gazeboflag boolean,
         graineryarea integer,
         graineryflag boolean,
         greenhousearea integer,
         greenhouseflag boolean,
         guesthousearea integer,
         guesthouseflag boolean,
         kennelarea integer,
         kennelflag boolean,
         leantoarea integer,
         leantoflag boolean,
         loadingplatformarea integer,
         loadingplatformflag boolean,
         milkhousearea integer,
         milkhouseflag boolean,
         outdoorkitchenfireplaceflag boolean,
         poolhousearea integer,
         poolhouseflag boolean,
         poultryhousearea integer,
         poultryhouseflag boolean,
         quonsetarea integer,
         quonsetflag boolean,
         shedarea integer,
         shedcode character varying(4),
         siloarea integer,
         siloflag boolean,
         stablearea integer,
         stableflag boolean,
         storagebuildingarea integer,
         storagebuildingflag boolean,
         utilitybuildingarea integer,
         utilitybuildingflag boolean,
         polestructurearea integer,
         polestructureflag boolean,
         communityrecroomflag boolean,
         publicationdate date,
         parcelshellrecord boolean,
         rawsitusaddress character varying(100),
         rawsituscity character varying(50),
         rawsitusstate character varying(2),
         rawsituszip character varying(5),
         rawsituszip4 character varying(4),
         rawsitushousenumber character varying(20),
         rawsitushousenumberfraction character varying(20),
         rawsitusstreetname character varying(100),
         rawsitusdirection character varying(10),
         rawsitusaddresssuffix character varying(25),
         rawsituspostdirection character varying(10),
         rawsitusunitprefix character varying(20),
         rawsitusunitvalue character varying(20),
         contactownermailaddressforeign boolean
        );
        """)

        # Load delta_raw_adm table
        fuzzy_matching.print_now('Loading delta_raw_adm table')
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
                situscounty,
                parcelnumberraw,
                propertyaddressfull,
                propertyaddresshousenumber,
                propertyaddressstreetdirection,
                propertyaddressstreetname,
                propertyaddressstreetsuffix,
                propertyaddressstreetpostdirection,
                propertyaddressunitprefix,
                propertyaddressunitvalue,
                propertyaddresscity,
                propertyaddressstate,
                propertyaddresszip,
                geoquality,
                partyowner1namefull,
                partyowner3namefull,
                partyowner1namefirst,
                partyowner1namelast,
                partyowner2namefirst,
                partyowner2namelast,
                partyowner3namefirst,
                partyowner3namelast,
                partyowner4namefirst,
                partyowner4namelast,
                contactownermailaddressfull,
                contactownermailaddresshousenumber,
                contactownermailaddressstreetdirection,
                contactownermailaddressstreetname,
                contactownermailaddressstreetsuffix,
                contactownermailaddressstreetpostdirection,
                contactownermailaddressunitprefix,
                contactownermailaddressunit,
                contactownermailaddresscity,
                contactownermailaddressstate,
                contactownermailaddresszip,
                contactownermailaddresszip4,
                contactownermailaddresscrrt,
                propertyusestandardized
                )
            );
            """, raw_file_in)
            """
            Delete raw_file_sed now to make more space. Don't confuse 
            raw_file_sed with raw_file.
            """
            #subprocess.check_call(f'rm {raw_file_sed}', shell=True)

        # Prepare delta_raw_adm table ss
        fuzzy_matching.print_now('Preparing delta_raw_adm table for SS')
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
        ALTER TABLE delta_raw_adm RENAME ContactOwnerMailAddressState TO state;""")
        if not skip_ss:
            # Export for ss
            fuzzy_matching.print_now('Exporting delta_raw_adm for SS')
            with connection.cursor('cursor_smarty') as cursor_smarty:
                cursor_smarty.execute("""
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
                    for row in cursor_smarty:
                        writer.writerow(row)

            # Process through smarty streets
            fuzzy_matching.print_now('Processing through SS')
            subprocess.check_call(f'./smartylist '
                f'-auth-id={ss_auth_id} '
                f'-auth-token={ss_auth_token} '
                f'-input={ss_in} '
                f'-output={ss_out} '
                f'-silent', shell=True)

        # Create the delta_ss table for ss data
        fuzzy_matching.print_now('Creating the delta_ss table for SS data')
        cursor_cs.execute("""
        drop table if exists delta_ss;
        CREATE TABLE delta_ss
        (
            admid bigint,
            street varchar,
            secondary varchar,
            city varchar,
            state varchar,
            blank varchar,
            sequence varchar,
            summary	varchar,
            addressee varchar,
            delivery_line_1 varchar,
            delivery_line_2 varchar,
            city_name varchar,
            state_abbreviation varchar,
            full_zipcode varchar,
            notes varchar,
            county_name varchar,
            rdi varchar,
            latitude numeric(10, 6),
            longitude numeric(10, 6),
            precision varchar,
            dpv_match_code varchar,
            dpv_footnotes varchar,
            footnotes varchar,
            county_fips varchar,
            record_type varchar,
            zip_type varchar,
            carrier_route varchar,
            congressional_district varchar,
            building_default_indicator varchar,
            elot_sequence varchar,
            elot_sort varchar,
            time_zone varchar,
            utc_offset varchar,
            dst varchar,
            dpv_cmra varchar,
            ews_match varchar,
            lacslink_indicator varchar,
            lacslink_code varchar,
            suitelink_match varchar,
            dpv_vacant varchar,
            active varchar,
            urbanization varchar,
            primary_number varchar,
            street_name varchar,
            street_predirection varchar,
            street_postdirection varchar,
            street_suffix varchar,
            secondary_number varchar,
            secondary_designator varchar,
            extra_secondary_number varchar,
            extra_secondary_designator varchar,
            pmb_designator varchar,
            pmb_number varchar,
            default_city_name varchar,
            zipcode varchar,
            plus4_code varchar,
            delivery_point varchar,
            delivery_point_check_digit varchar,
            last_line varchar,
            delivery_point_barcode varchar
        )""")

        # Load the ss data into delta_ss table
        fuzzy_matching.print_now('Loading the ss data into delta_ss table')
        with open(ss_out, 'r') as ss_out_in:
            cursor_cs.copy_expert(f'''
            COPY delta_ss
            FROM STDIN
            (
            FORMAT csv,
            DELIMITER E'\t',
            HEADER,
            QUOTE E'\b',
            ENCODING 'LATIN1',
            FORCE_NOT_NULL (
                admid,
                summary,
                delivery_line_1,
                city_name,
                state_abbreviation,
                county_name,
                rdi,
                latitude,
                longitude,
                precision,
                county_fips,
                primary_number,
                street_name,
                street_predirection,
                street_postdirection,
                street_suffix,
                secondary_number,
                secondary_designator,
                extra_secondary_number,
                extra_secondary_designator,
                zipcode)
            );''', ss_out_in)

        fuzzy_matching.print_now('Analyzing delta_ss and delta_raw_adm')
        cursor_cs.execute("""
        -- create unique index on delta_ss (admid);
        -- create unique index on delta_raw_adm (admid);
        analyze delta_raw_adm;
        analyze delta_ss;
        drop table if exists delta_and_ss;
        """)
        # Create table that stitches the ss and delta together.
        fuzzy_matching.print_now('Creating and loading delta_and_ss table')
        cursor_cs.execute("""
        create table delta_and_ss as
            select
                delta_raw_adm.*,
                sequence,
                summary,
                addressee,
                delivery_line_1,
                delivery_line_2,
                city_name,
                state_abbreviation,
                full_zipcode,
                notes,
                county_name,
                rdi,
                latitude,
                longitude,
                precision,
                dpv_match_code,
                dpv_footnotes,
                footnotes,
                county_fips,
                record_type,
                zip_type,
                carrier_route,
                congressional_district,
                building_default_indicator,
                elot_sequence,
                elot_sort,
                time_zone,
                utc_offset,
                dst,
                dpv_cmra,
                ews_match,
                lacslink_indicator,
                lacslink_code,
                suitelink_match,
                dpv_vacant,
                active,
                urbanization,
                primary_number,
                street_name,
                street_predirection,
                street_postdirection,
                street_suffix,
                secondary_number,
                secondary_designator,
                extra_secondary_number,
                extra_secondary_designator,
                pmb_designator,
                pmb_number,
                default_city_name,
                zipcode,
                plus4_code,
                delivery_point,
                delivery_point_check_digit,
                last_line,
                delivery_point_barcode
            from delta_ss
                join delta_raw_adm on delta_ss.admid = delta_raw_adm.admid;""")

        # Drop delta_ss because it's not used anymore.
        fuzzy_matching.print_now('Dropping table delta_ss')
        cursor_cs.execute('Drop table if exists delta_ss;')

        fuzzy_matching.print_now('Analyzing delta_and_ss')
        cursor_cs.execute('''
        analyze delta_and_ss;
        ''')

        # Drop delta_raw_adm
        cursor_cs.execute("""
        drop table if exists delta_raw_adm;
        """
                          )

        # Create 3 delta relational tables
        fuzzy_matching.print_now('Creating 3 delta relational tables')
        cursor_cs.execute("""
        DROP TABLE IF EXISTS delta_jmb_identity CASCADE;
        CREATE TEMPORARY TABLE delta_jmb_identity
        (
         id SERIAL,
             --PRIMARY KEY,
         mail_address VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
         mail_city VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
         mail_state VARCHAR(32) NOT NULL DEFAULT ''::VARCHAR,
         mail_zipcode VARCHAR(12) NOT NULL DEFAULT ''::VARCHAR,
         mail_verified VARCHAR(32) NOT NULL DEFAULT ''::VARCHAR,
         mail_longitude NUMERIC(10,6),
         mail_latitude NUMERIC(10,6),
         mail_geoquality VARCHAR(20) NOT NULL DEFAULT ''::VARCHAR,
         mail_location GEOMETRY(Point,4326),
         rdi VARCHAR(12) NOT NULL DEFAULT ''::VARCHAR,
         primary_name VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
         rental_count SMALLINT,
         UNIQUE (mail_address, mail_city, mail_state)
        );
    
    
        DROP TABLE IF EXISTS delta_jmb_entity CASCADE;
        CREATE TEMPORARY TABLE delta_jmb_entity
        (
          id SERIAL,
              --PRIMARY KEY,
          owner VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
          owner1_firstname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          owner1_lastname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          owner2_firstname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          owner2_lastname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          coowner VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
          coowner1_firstname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          coowner1_lastname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          coowner2_firstname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          coowner2_lastname VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          identity_id INTEGER
            --Commented on purpose. See note at bottom about constraints.
            --NOT NULL REFERENCES delta_jmb_identity(id) ON DELETE CASCADE
        );
    
    
        DROP TABLE IF EXISTS delta_jmb_parcel;
        CREATE TEMPORARY TABLE delta_jmb_parcel
        (
          id SERIAL,
              --PRIMARY KEY,
          admid bigint NOT NULL,
              --UNIQUE,
          parcel_number VARCHAR(60) NOT NULL DEFAULT ''::VARCHAR,
          parcel_county VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          parcel_fips character(5) NOT NULL,
          parcel_address1 VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
          parcel_address2 VARCHAR(128) NOT NULL DEFAULT ''::VARCHAR,
          parcel_city VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          parcel_state VARCHAR(2) NOT NULL DEFAULT ''::VARCHAR,
          parcel_zipcode VARCHAR(5) NOT NULL DEFAULT ''::VARCHAR,
          parcel_privacy BOOLEAN,
          parcel_longitude NUMERIC(10,6),
          parcel_latitude NUMERIC(10,6),
          parcel_geoquality VARCHAR(20) NOT NULL DEFAULT ''::VARCHAR,
          parcel_location GEOMETRY(Point,4326),
          owner_occupied BOOLEAN,
          record_type VARCHAR(50) NOT NULL DEFAULT ''::VARCHAR,
          land_use VARCHAR(4) NOT NULL DEFAULT ''::VARCHAR,
          acres numeric(18,7),
          sqft_total INTEGER,
          unit_count INTEGER,
          building_count INTEGER,
          bedroom_count SMALLINT,
          bathroom_count NUMERIC(7,3),
          pool SMALLINT,
          guesthouse BOOLEAN,
          year_built SMALLINT,
          last_sale DATE,
          last_price BIGINT,
          year_assessed smallint,
          total_value BIGINT,
          rental_flag BOOLEAN,
          entity_id INTEGER,
            --Commented on purpose. See note at bottom about constraints.
              --NOT NULL
              --REFERENCES delta_jmb_entity(id) ON DELETE CASCADE,
          identity_id INTEGER
            --Commented on purpose. See note at bottom about constraints.
              --NOT NULL
              --REFERENCES delta_jmb_identity(id) ON DELETE CASCADE
        );
        """)

        # Populate delta relational tables
        fuzzy_matching.print_now('Populating delta relational tables')
        fuzzy_matching.print_now('Populating delta_jmb_identity')
        cursor_cs.execute("""
        INSERT INTO delta_jmb_identity(
            mail_address,
            mail_city,
            mail_state,
            mail_zipcode,
            mail_verified,
            mail_longitude,
            mail_latitude,
            mail_geoquality,
            rdi)
            SELECT DISTINCT
            delivery_line_1,
            city_name,
            state_abbreviation,
            zipcode,
            summary,
            longitude,
            latitude,
            precision,
            rdi
        FROM delta_and_ss
            --Added on conflict clause because there were a few "violates unique constraint" on
            --jmb_identity unique (address,city,state)
        on conflict do nothing;
         --(local) [2019-04-29 17:46:30] 1059965 rows affected in 1 m 57 s 130 ms
         --(stage) Started at 4:59 - Finished at 4:59
         """)

        fuzzy_matching.print_now('Populating delta_jmb_entity')
        cursor_cs.execute("""
        INSERT INTO delta_jmb_entity(
        owner,
        coowner,
        owner1_firstname,
        owner1_lastname,
        owner2_firstname,
        owner2_lastname,
        coowner1_firstname,
        coowner1_lastname,
        coowner2_firstname,
        coowner2_lastname,
        identity_id) SELECT DISTINCT
            a.partyowner1namefull,
            a.partyowner3namefull,
            a.partyowner1namefirst,
            a.partyowner1namelast,
            a.partyowner2namefirst,
            a.partyowner2namelast,
            a.partyowner3namefirst,
            a.partyowner3namelast,
            a.partyowner4namefirst,
            a.partyowner4namelast,
            i.id
        FROM delta_and_ss AS a
        LEFT JOIN delta_jmb_identity as i
            ON a.delivery_line_1 = i.mail_address
            AND a.city_name=i.mail_city
            AND a.state_abbreviation = i.mail_state;
         --(local) [2019-04-29 17:46:30] 1059965 rows affected in 1 m 57 s 130 ms
         --(stage) Started at 4:59 - Finished at 4:59
         """)

        fuzzy_matching.print_now('Populating delta_jmb_parcel')
        cursor_cs.execute("""
          INSERT INTO delta_jmb_parcel(
              admid,
              parcel_number,
              parcel_county,
              parcel_fips,
              parcel_address1,
              parcel_address2,
              parcel_city,
              parcel_state,
              parcel_zipcode,
              parcel_longitude,
              parcel_latitude,
              parcel_geoquality,
              owner_occupied,
              record_type,
              land_use,
              acres,
              sqft_total,
              unit_count,
              building_count,
              bedroom_count,
              bathroom_count,
              pool,
              guesthouse,
              year_built,
              last_sale,
              last_price,
              year_assessed,
              total_value,
              identity_id,
              entity_id
          )
          SELECT
              a.admid,
              a.parcelnumberraw,
              a.situscounty,
              a.situsstatecountyfips,
              BTRIM(REGEXP_REPLACE(CONCAT(
                  PropertyAddressHouseNumber, ' ',
                  PropertyAddressStreetDirection, ' ',
                  PropertyAddressStreetName, ' ',
                  PropertyAddressStreetSuffix, ' ',
                  PropertyAddressStreetPostDirection), '\s+', ' ', 'g')) AS Address1,
              BTRIM(REGEXP_REPLACE(CONCAT(
                  PropertyAddressUnitPrefix, ' ',
                  PropertyAddressUnitValue), '\s+', ' ', 'g')) AS Address2,
              a.propertyaddresscity,
              a.propertyaddressstate,
              a.propertyaddresszip,
              a.propertylongitude,
              a.propertylatitude,
              a.geoquality,
              a.statusowneroccupiedflag,
              a.propertyusegroup,
              a.propertyusestandardized,
              a.arealotacres,
              a.areabuilding,
              a.unitscount,
              a.buildingscount,
              a.bedroomscount,
              a.bathcount,
              a.pool,
              a.guesthouseflag,
              a.yearbuilt,
              a.assessorlastsaledate,
              a.assessorlastsaleamount,
              a.taxyearassessed,
              a.taxassessedvaluetotal,
              i.id,
              e.id
          FROM delta_and_ss as a
    
          LEFT JOIN delta_jmb_identity as i
              ON a.delivery_line_1 = i.mail_address
              AND a.city_name=i.mail_city
              AND a.state_abbreviation = i.mail_state
    
          LEFT JOIN delta_jmb_entity AS e
              ON a.partyowner1namefull = e.owner
              AND a.partyowner3namefull = e.coowner
              AND a.partyowner1namefirst = e.owner1_firstname
              AND a.partyowner1namelast = e.owner1_lastname
              AND a.partyowner2namefirst = e.owner2_firstname
              AND a.partyowner2namelast = e.owner2_lastname
              AND a.partyowner3namefirst = e.coowner1_firstname
              AND a.partyowner3namelast = e.coowner1_lastname
              AND a.partyowner4namefirst = e.coowner2_firstname
              AND a.partyowner4namelast = e.coowner2_lastname
              AND i.id = e.identity_id;
              """)

        # Drop delta_and_ss because it's not used anymore.
        fuzzy_matching.print_now('Dropping table delta_and_ss')
        cursor_cs.execute('Drop table if exists delta_and_ss;')

        # Add the keys and constraints that I originally didn't set because it slows down the queries.
        fuzzy_matching.print_now('Adding keys and constraints to delta relational tables')
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
        Create unique index delta_jmb_parcel_admid_key on delta_jmb_parcel (admid);""")

        # Set location field for parcel and identity
        fuzzy_matching.print_now('Setting location field for parcel and identity')
        cursor_cs.execute("""
        UPDATE delta_jmb_parcel
        SET parcel_location=st_SetSrid(st_MakePoint(parcel_longitude, parcel_latitude), 4326);
        --(local) [2019-04-29 17:58:06] 1474117 rows affected in 1 m 50 s 77 ms
        --(stage) Took 7 seconds
    
        UPDATE delta_jmb_identity
        SET mail_location=st_SetSrid(st_MakePoint(mail_longitude, mail_latitude), 4326);
        --(local) [2019-04-29 17:59:27] 1059965 rows affected in 1 m 20 s 727 ms
        --(stage) Took 8 seconds
        """)

        """
        I have to make sure that the sequence is started correctly with the 
        below queries. Here's the error I was getting:
         
        psycopg2.IntegrityError: duplicate key value violates unique 
        constraint "jmb_entity_pkey" DETAIL:  Key (id)=(2) already exists.
        """
        # TODO: Find out why this is happening that the sequence isn't starting on the next id
        cursor_cs.execute("""
        SELECT setval(pg_get_serial_sequence('jmb_identity', 'id'), coalesce(max(id),0) + 1, false)
        FROM jmb_identity;
    
        SELECT setval(pg_get_serial_sequence('jmb_entity', 'id'), coalesce(max(id),0) + 1, false)
        FROM jmb_entity;
    
        SELECT setval(pg_get_serial_sequence('jmb_parcel', 'id'), coalesce(max(id),0) + 1, false)
        FROM jmb_parcel;
        """)

        # Add multi-column index to jmb_entity to speed up selects and inserts.
        fuzzy_matching.print_now('Adding jmb_entity_uniqueness_idx on entity columns')
        cursor_cs.execute("""
        drop index if exists jmb_entity_uniqueness_idx;
        create index jmb_entity_uniqueness_idx on jmb_entity (
                                    owner,
                                    owner1_firstname,
                                    owner1_lastname,
                                    owner2_firstname,
                                    owner2_lastname,
                                    coowner,
                                    coowner1_firstname,
                                    coowner1_lastname,
                                    coowner2_firstname,
                                    coowner2_lastname,
                                    identity_id
        );
        """)

        fuzzy_matching.print_now('Starting to process delta. Selecting delta rows..')
        """
        This query selects all the columns from delta_jmb tables joined 
        together. This will be the delta data that is processed row by row
        to update jmb.
        """
        query = '''
        select
        /*TODO: Change ss_and_delta in delta_load.py, add coalesces there
         *so that the delta_jmb tables don't contain null values to begin 
         *with.
         */
            coalesce(owner, '') as owner,
            coalesce(owner1_firstname, '') as owner1_firstname,
            coalesce(owner1_lastname, '') as owner1_lastname,
            coalesce(owner2_firstname, '') as owner2_firstname,
            coalesce(owner2_lastname, '') as owner2_lastname,
            coalesce(coowner, '') as coowner,
            coalesce(coowner1_firstname, '') as coowner1_firstname,
            coalesce(coowner1_lastname, '') as coowner1_lastname,
            coalesce(coowner2_firstname, '') as coowner2_firstname,
            coalesce(coowner2_lastname, '') as coowner2_lastname,
            coalesce(mail_address, '') as mail_address,
            coalesce(mail_city, '') as mail_city,
            coalesce(mail_state, '') as mail_state,
            mail_zipcode,
            coalesce(mail_verified, '') as mail_verified,
            mail_longitude,
            mail_latitude,
            coalesce(mail_geoquality, '') as mail_geoquality,
            mail_location,
            coalesce(rdi, '') as rdi,
            coalesce(primary_name, '') as primary_name,
            rental_count,
            admid,
            coalesce(parcel_number, '') as parcel_number,
            coalesce(parcel_county, '') as parcel_county,
            coalesce(parcel_fips, '') as parcel_fips,
            coalesce(parcel_address1, '') as parcel_address1,
            coalesce(parcel_address2, '') as parcel_address2,
            coalesce(parcel_city, '') as parcel_city,
            coalesce(parcel_state, '') as parcel_state,
            coalesce(parcel_zipcode, '') as parcel_zipcode,
            parcel_privacy,
            parcel_longitude,
            parcel_latitude,
            coalesce(parcel_geoquality, '') as parcel_geoquality,
            parcel_location,
            owner_occupied,
            coalesce(record_type, '') as record_type,
            coalesce(land_use, '') as land_use,
            acres,
            sqft_total,
            unit_count,
            building_count,
            bedroom_count,
            bathroom_count,
            pool,
            guesthouse,
            year_built,
            last_sale,
            last_price,
            year_assessed,
            total_value,
            rental_flag
         from delta_jmb_parcel
        join delta_jmb_identity identity2 on delta_jmb_parcel.identity_id = identity2.id
        join delta_jmb_entity je on delta_jmb_parcel.entity_id = je.id;
        '''
        cursor_ss.execute(query)
        fuzzy_matching.print_now('Starting first pass for jmb_entity and jmb_identity inserts')

        """
        For every row in delta get the existing or new identity_id and 
        entity_id, store those in a dictionary called relationships with
        {admid: identity_id, entity_id} and do inserts of new 
        identities and entities.
        """
        for i, row in enumerate(cursor_ss):
            if i % 1000 == 0:
                # print(f'--working on row {i}', end='\r')
                fuzzy_matching.print_now(f'--working on row {i}')
            identity_columns = (
                row.mail_address,
                row.mail_city,
                row.mail_state,
                row.mail_zipcode,
                row.mail_verified,
                row.mail_longitude,
                row.mail_latitude,
                row.mail_geoquality,
                row.mail_location,
                row.rdi,
            )
            entity_columns = (
                row.owner,
                row.owner1_firstname,
                row.owner1_lastname,
                row.owner2_firstname,
                row.owner2_lastname,
                row.coowner,
                row.coowner1_firstname,
                row.coowner1_lastname,
                row.coowner2_firstname,
                row.coowner2_lastname,
            )

            # Select to check if the delta_identity already exists
            cursor_cs.execute('''
            select *
            from jmb_identity
            where
            mail_address = %s
            and mail_city = %s
            and mail_state = %s
            /*Commented because only mail_address, mail_city, and mail_state
             *are used to uniquely identify an identity. If adding more 
             *columns to this query you may need to adjust the index so 
             *performance doesn't drop off a cliff.
             */
            --and mail_zipcode = 
            --and mail_verified = 
            --and mail_longitude = 
            --and mail_latitude = 
            --and mail_geoquality = 
            --and mail_location = 
            --and rdi = 
            ''', (identity_columns[0], identity_columns[1], identity_columns[2],))
            identity_result = cursor_cs.fetchall()
            if len(identity_result) > 1:
                print('ERROR: There were multiple identities that matched.')

            # If identity already exists, get id of existing identity
            if identity_result:
                identity_id = identity_result[0].id
                """
                If the identity that matches the select is set to inactive
                set that identity back to active to avoid a parcel being
                joined to an inactive identity.
                """
                if identity_result[0].inactive == True:
                    cursor_cs.execute('''
                    Update jmb_identity
                    set inactive = null
                    where id = %s
                    ''', (identity_id,))
            # Else identity doesn't exist, insert and get id of new identity
            else:
                cursor_cs.execute(
                    '''
                    insert into jmb_identity (
                    mail_address,
                    mail_city,
                    mail_state,
                    mail_zipcode,
                    mail_verified,
                    mail_longitude,
                    mail_latitude,
                    mail_geoquality,
                    mail_location,
                    rdi,
                    primary_name,
                    rental_count
                    ) values (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    '',
                    0 
                    ) returning id
                    ''', (*identity_columns,)
                )
                identity_id = cursor_cs.fetchall()[0].id

            # Select to check if the delta_entity already exists
            cursor_cs.execute('''
            select *
            from jmb_entity
            where 
            /*If adding or deleting any columns from this query keep in mind
             *that it may mess up the ability to use the 
             *delta_jmb_entity_uniqueness_idx and performance may suffer.
             */
                owner = %s
                and owner1_firstname = %s
                and owner1_lastname = %s
                and owner2_firstname = %s
                and owner2_lastname = %s
                and coowner = %s
                and coowner1_firstname = %s
                and coowner1_lastname = %s
                and coowner2_firstname = %s
                and coowner2_lastname = %s
                and identity_id = %s
            ''', (*entity_columns, identity_id))
            entity_result = cursor_cs.fetchall()
            if len(entity_result) > 1:
                print('ERROR: There were multiple entities that matched.')
            # If entity already exists, get id of existing entity
            if entity_result:
                entity_id = entity_result[0].id
            # If entity doesn't exist, insert and get id of new entity
            else:
                cursor_cs.execute(
                    '''
                    insert into jmb_entity (
                    owner,
                    owner1_firstname,
                    owner1_lastname,
                    owner2_firstname,
                    owner2_lastname,
                    coowner,
                    coowner1_firstname,
                    coowner1_lastname,
                    coowner2_firstname,
                    coowner2_lastname,
                    identity_id
                    ) values (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    ) returning id
                    ''', (*entity_columns, identity_id)
                )
                entity_id = cursor_cs.fetchall()[0].id
            # Insert this row's identity_id and entity_id into relationships
            relationships[row.admid] = [entity_id, identity_id]

        # Reset the server-side cursor to postion 0 to reiterate over rows.
        cursor_ss.scroll(0, mode='absolute')

        # create set of distinct admids
        fuzzy_matching.print_now('Creating set of distinct admids')
        cursor_cs.execute('''
        select distinct admid from jmb_parcel;
        ''')
        admids = set([x[0] for x in cursor_cs.fetchall()])

        fuzzy_matching.print_now('Second pass to update or insert into jmb_parcel')
        """
        Do a second pass to either insert new parcels or update old 
        parcels. Uses delta row info and also the mapping in dictionary 
        for foreign keys to identity and entity.
        """
        for i, row in enumerate(cursor_ss):
            if i % 1000 == 0:
                fuzzy_matching.print_now(f'--working on row {i}')
                # print(f'--working on row {i}', end='\r')
            raw_parcel_match_address = ' '.join(
                (row.parcel_address1,
                 row.parcel_address2,
                 row.parcel_city,
                 row.parcel_state))
            parcel_match_address = fuzzy_matching.normalize_address(raw_parcel_match_address)
            # if the parcel(admid) already in jmb_parcel then update
            if row.admid in admids:
                cursor_cs.execute(
                    '''
                    update jmb_parcel set (
                    parcel_number,
                    parcel_county,
                    parcel_fips,
                    parcel_address1,
                    parcel_address2,
                    parcel_city,
                    parcel_state,
                    parcel_zipcode,
                    parcel_privacy,
                    parcel_longitude,
                    parcel_latitude,
                    parcel_geoquality,
                    parcel_location,
                    owner_occupied,
                    record_type,
                    land_use,
                    acres,
                    sqft_total,
                    unit_count,
                    building_count,
                    bedroom_count,
                    bathroom_count,
                    pool,
                    guesthouse,
                    year_built,
                    last_sale,
                    last_price,
                    year_assessed,
                    total_value,
                    rental_flag,
                    parcel_match_address,
                    entity_id,
                    identity_id
                    ) = (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    )
                    where admid = %s
                    ''', (
                        row.parcel_number,
                        row.parcel_county,
                        row.parcel_fips,
                        row.parcel_address1,
                        row.parcel_address2,
                        row.parcel_city,
                        row.parcel_state,
                        row.parcel_zipcode,
                        row.parcel_privacy,
                        row.parcel_longitude,
                        row.parcel_latitude,
                        row.parcel_geoquality,
                        row.parcel_location,
                        row.owner_occupied,
                        row.record_type,
                        row.land_use,
                        row.acres,
                        row.sqft_total,
                        row.unit_count,
                        row.building_count,
                        row.bedroom_count,
                        row.bathroom_count,
                        row.pool,
                        row.guesthouse,
                        row.year_built,
                        row.last_sale,
                        row.last_price,
                        row.year_assessed,
                        row.total_value,
                        row.rental_flag,
                        parcel_match_address,
                        relationships[row.admid][0],
                        relationships[row.admid][1],
                        row.admid
                    )
                )
            # Else the parcel(admid) isn't already in jmb_parcel then insert
            else:
                """
                Future optimization: When we start ingesting large data sets 
                while adding states, the number of inserts will be very 
                high. Do bulk inserts by writing inserts out to file in 
                loop and then using cursor.copy_expert(), reading the file 
                into STDIN, to bulk insert into RDS tables.
                """
                cursor_cs.execute(
                    '''
                    INSERT into jmb_parcel (
                    admid,
                    parcel_number,
                    parcel_county,
                    parcel_fips,
                    parcel_address1,
                    parcel_address2,
                    parcel_city,
                    parcel_state,
                    parcel_zipcode,
                    parcel_privacy,
                    parcel_longitude,
                    parcel_latitude,
                    parcel_geoquality,
                    parcel_location,
                    owner_occupied,
                    record_type,
                    land_use,
                    acres,
                    sqft_total,
                    unit_count,
                    building_count,
                    bedroom_count,
                    bathroom_count,
                    pool,
                    guesthouse,
                    year_built,
                    last_sale,
                    last_price,
                    year_assessed,
                    total_value,
                    rental_flag,
                    parcel_match_address,
                    entity_id,
                    identity_id
                    ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    )
                    ''', (
                        row.admid,
                        row.parcel_number,
                        row.parcel_county,
                        row.parcel_fips,
                        row.parcel_address1,
                        row.parcel_address2,
                        row.parcel_city,
                        row.parcel_state,
                        row.parcel_zipcode,
                        row.parcel_privacy,
                        row.parcel_longitude,
                        row.parcel_latitude,
                        row.parcel_geoquality,
                        row.parcel_location,
                        row.owner_occupied,
                        row.record_type,
                        row.land_use,
                        row.acres,
                        row.sqft_total,
                        row.unit_count,
                        row.building_count,
                        row.bedroom_count,
                        row.bathroom_count,
                        row.pool,
                        row.guesthouse,
                        row.year_built,
                        row.last_sale,
                        row.last_price,
                        row.year_assessed,
                        row.total_value,
                        row.rental_flag,
                        parcel_match_address,
                        relationships[row.admid][0],
                        relationships[row.admid][1]
                    )
                )

    # Open and read delete file and delete parcels
    fuzzy_matching.print_now('Processing delta_delete file')
    with open(delete_file, 'r') as f:
        for i, admid in enumerate(f.readlines()):
            if i > 0:
                cursor_cs.execute('''
                select id from jmb_parcel where admid = %s
                ''', (admid,))
                result = cursor_cs.fetchone()
                if result:
                    parcel_id = result[0]
                    cursor_cs.execute('''
                    update jmb_postcard set parcel_id = null where parcel_id = %s
                    ''', (parcel_id,))
                cursor_cs.execute('''
                delete 
                from jmb_parcel 
                where admid = %s
                ''', (admid,))
        fuzzy_matching.print_now(f'Deleted {i} parcels.')

        # Set inactive = True on identities that no longer have parcels
        fuzzy_matching.print_now('Setting inactive=True on identities with no parcels')
        cursor_cs.execute('''
        update jmb_identity 
        set inactive = True
        where id in (
        select jmb_identity.id
        from
            jmb_identity
            left join jmb_parcel on jmb_parcel.identity_id = jmb_identity.id
        where jmb_parcel.id is null);
        ''')

        # delete entities that no longer have parcels
        fuzzy_matching.print_now('Deleting entities with no parcels')
        cursor_cs.execute('''
        delete from jmb_entity where id in (
        select jmb_entity.id
        from
            jmb_entity
            left join jmb_parcel on jmb_parcel.entity_id = jmb_entity.id
        where jmb_parcel.id is null);
        ''')

        # Commit the changes
        fuzzy_matching.print_now('Committing changes...')
        connection.commit()
fuzzy_matching.print_now('Finished!')
