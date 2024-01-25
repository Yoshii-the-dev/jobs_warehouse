
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf
import os
from dotenv import load_dotenv
import logging
from pyflink.common import Configuration
import sys
from datetime import datetime
import spacy
import locationtagger
from unidecode import unidecode
import nltk
import sqlite3
import udfs
import time


def raw_html_processing():
    
    load_dotenv('.env')
    APP_DB_USER = os.getenv('APP_DB_USER')
    APP_DB_PASS = os.getenv('APP_DB_PASS')
#========================= SET UP PROCESSING CONFIG ============================================
    SRC_DIR = os.path.dirname(os.path.realpath(__file__))
    jar_files = [
        "flink-sql-connector-postgres-cdc-2.4.0.jar",
        "postgresql-42.6.0.jar",
        "flink-connector-jdbc-3.1.0-1.17.jar"
    ]
    jar_paths = tuple([f"file:///{os.path.join(SRC_DIR, 'lib' ,jar)}" for jar in jar_files])
    jar_paths = tuple(path.replace('\\','/') for path in jar_paths)

    config = Configuration()
    config.set_string("python.profile.enabled", "true")
    config.set_string("table.dml-sync", "true")
    config.set_string("pipeline.jars", ";".join(jar_paths))
    env_settings = EnvironmentSettings \
        .new_instance() \
        .in_batch_mode() \
        .with_configuration(config) \
        .build()
    table_env = TableEnvironment.create(env_settings)

#===================== REGISTER UDFS ==========================================================
    table_env.create_temporary_function('role_from_title', udfs.role_from_title)
    table_env.create_temporary_function('list_of_tools', udfs.list_of_tools)
    table_env.create_temporary_function('geographical_attributes', udfs.geographical_attributes)
    #table_env.create_temporary_function('generate_incremental_index', udfs.generate_incremental_index)

#===================== DEFINE DATA SOURCES AND SINKS ===========================================
    table_env.execute_sql(f'''
    CREATE TABLE first_stage_raw (
        id INT,
        title STRING,
        company STRING,
        location STRING,
        employment_type STRING,
        date_posted DATE,
        job_description STRING,
        metadata_lastupdated TIMESTAMP,
        metadata_application_link STRING,
        metadata_source STRING,
        PRIMARY KEY (title, company, location) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'landing_zone_schema.raw_jobs_table',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'    
    );
    '''
    )

    table_env.execute_sql(f'''
    CREATE TABLE landing_zone_schema_clean_jobs_table (
        id INT,
        title STRING,
        role_key STRING,
        role STRING,
        company_key STRING,
        company STRING,
        location_key STRING,
        location STRING,
        country STRING,
        city STRING,
        employment_type_key STRING,
        employment_type STRING,
        date_posted DATE,
        job_description STRING,
        toolset_list STRING,
        metadata_lastupdated TIMESTAMP,
        metadata_application_link STRING,
        metadata_source STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'landing_zone_schema.clean_jobs_table',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'    
    );
    '''
    )
    
    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_role_dim (
        role_pk STRING,
        role STRING,
        PRIMARY KEY (role_pk) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.role_dim',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'    
    );
    '''
    )
   
    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_employment_type_dim (
        employment_type_pk STRING,
        employment_type STRING,
        PRIMARY KEY (employment_type_pk) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.employment_type_dim',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'      
    );
    '''
    )

    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_location_dim (
        location_pk STRING,
        country STRING,
        city STRING,
        PRIMARY KEY (location_pk) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.location_dim',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'    
    );
    '''
    )

#    table_env.execute_sql('''
#    CREATE TABLE presentation_zone_schema.seniority_dim (
#        seniority_pk INT,
#        seniority STRING,
#        PRIMARY KEY (seniority_pk) NOT ENFORCED
#    ) WITH (
#        'connector' = 'jdbc',
#        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
#        'table-name' = 'presentation_zone_schema.seniority_dim',
#        'driver' = 'org.postgresql.Driver',
#        'username' = 'data_wrangler_rw',
#        'password' = 'data_wrangler_rw123'    
#    );
#    '''
#    )

    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_company_dim (
        company_pk STRING,
        company STRING,
        PRIMARY KEY (company_pk) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.company_dim',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'      
    );
    '''
    )

    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_fact_less_table (
        date_posted_fk DATE,
        role_fk STRING,
        company_fk STRING,
        employment_type_fk STRING,
        location_fk STRING,
        toolset_list STRING,
        last_updated TIMESTAMP,                
        PRIMARY KEY (date_posted_fk, role_fk, company_fk, employment_type_fk, location_fk) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.fact_less_table',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'      
    );
    '''
    )
    table_env.execute_sql(f'''
    CREATE TABLE presentation_zone_schema_daily_snap_ads_number (
        snap_date DATE,
        open_ads BIGINT NOT NULL,
        PRIMARY KEY (snap_date) NOT ENFORCED              
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgresdb_container:5432/jobs_warehouse',
        'table-name' = 'presentation_zone_schema.daily_snap_ads_number',
        'driver' = 'org.postgresql.Driver',
        'username' = '{APP_DB_USER}',
        'password' = '{APP_DB_PASS}'    
    );
    '''
    )
# ============================== POPULATE DIMENSIONS AND FACTS ======================================================
    table_env.execute_sql('''
        INSERT INTO landing_zone_schema_clean_jobs_table (id, title, role_key, role, company_key, company, location_key, location, country, 
                                                        city, employment_type_key, employment_type, date_posted, job_description, toolset_list,
                                                        metadata_lastupdated, metadata_application_link, metadata_source)
        SELECT
            id,
            title,
            LEFT(MD5(role_from_title(title)), 3) as role_key,
            role_from_title(title) as role,
            LEFT(MD5(company), 5) as company_key,
            company,
            LEFT(MD5(location), 5) as location_key,
            location,
            CAST(geographical_attributes(location).country AS STRING) as country,
            CAST(geographical_attributes(location).city AS STRING) as city,
            LEFT(MD5(employment_type), 3) as employment_type_key,    
            employment_type,
            date_posted,
            job_description,
            list_of_tools(job_description) as toolset_list,
            metadata_lastupdated,
            metadata_application_link,
            metadata_source
        FROM first_stage_raw
    ''')

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_role_dim (role_pk, role)
        SELECT DISTINCT role_key, role
        FROM landing_zone_schema_clean_jobs_table
    '''
    )

    #table_env.execute_sql('''UPDATE landing_zone_schema_clean_jobs_table as lz SET role_key=
    #                      (SELECT role_pk FR0M presentation_zone_schema_role_dim as rd WHERE rd.role = lz.role)''')

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_employment_type_dim (employment_type_pk, employment_type)
        SELECT DISTINCT employment_type_key, employment_type
        FROM landing_zone_schema_clean_jobs_table
    '''
    )

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_company_dim (company_pk, company)
        SELECT DISTINCT company_key, company
        FROM landing_zone_schema_clean_jobs_table  
    '''
    )

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_location_dim (location_pk, country, city)
        SELECT DISTINCT location_key, country, city 
        FROM landing_zone_schema_clean_jobs_table  
    '''                      
    )
#    table_env.execute_sql('''
#        INSERT INTO presentation_zone_schema.seniority_dim
#        SELECT DISTINCT seniority
#        FROM landing_zone_schema.clean_jobs_table

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_fact_less_table (date_posted_fk, role_fk, company_fk, employment_type_fk, location_fk, toolset_list, last_updated)
        SELECT
            lz.date_posted,
            rd.role_pk,
            cd.company_pk,
            ed.employment_type_pk,
            ld.location_pk,
            lz.toolset_list,
            lz.metadata_lastupdated
        FROM 
            landing_zone_schema_clean_jobs_table as lz
        JOIN presentation_zone_schema_role_dim as rd on rd.role_pk = lz.role_key
        JOIN presentation_zone_schema_company_dim as cd on cd.company_pk = lz.company_key
        JOIN presentation_zone_schema_employment_type_dim as ed on ed.employment_type_pk = lz.employment_type_key
        JOIN presentation_zone_schema_location_dim as ld on ld.location_pk = lz.location_key  
    '''
    )

    table_env.execute_sql('''
        INSERT INTO presentation_zone_schema_daily_snap_ads_number (snap_date, open_ads)
        SELECT
            CURRENT_DATE,
            COUNT(*)
        FROM presentation_zone_schema_fact_less_table
        WHERE CAST(last_updated AS DATE) = CURRENT_DATE
    '''
    )

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    raw_html_processing()

