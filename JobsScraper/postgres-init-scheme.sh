#!/bin/bash

set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $APP_DB_USER WITH PASSWORD '$APP_DB_PASS';
  CREATE DATABASE $APP_DB_NAME;
  \connect $APP_DB_NAME $POSTGRES_USER
  GRANT ALL PRIVILEGES ON DATABASE $APP_DB_NAME TO $APP_DB_USER;
  CREATE SCHEMA landing_zone_schema;
  CREATE SCHEMA presentation_zone_schema;
  GRANT ALL PRIVILEGES ON SCHEMA landing_zone_schema TO $APP_DB_USER;
  GRANT ALL PRIVILEGES ON SCHEMA presentation_zone_schema TO $APP_DB_USER;
  GRANT TRUNCATE ON ALL TABLES IN SCHEMA landing_zone_schema TO $APP_DB_USER;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA landing_zone_schema TO $APP_DB_USER;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA presentation_zone_schema TO $APP_DB_USER;
  \connect $APP_DB_NAME $APP_DB_USER
  BEGIN;
    CREATE TABLE IF NOT EXISTS landing_zone_schema.raw_jobs_table (
      id SERIAL,
      title VARCHAR(100),
      company VARCHAR(50),
      location VARCHAR(100),
      employment_type VARCHAR(30),
      date_posted DATE,
      job_description TEXT,
      metadata_lastupdated TIMESTAMP,
      metadata_application_link VARCHAR(255),
      metadata_source VARCHAR(100),
      PRIMARY KEY(title, company, location)
  );
    CREATE TABLE IF NOT EXISTS landing_zone_schema.clean_jobs_table (
	    id SERIAL,
	    title VARCHAR(100),
      role_key VARCHAR(3),
      role VARCHAR(50),
      company_key VARCHAR(5),
      company VARCHAR(100),
      location_key VARCHAR(5),
      location VARCHAR(100),
      country VARCHAR(50),
      city VARCHAR(50),
      employment_type_key VARCHAR(3),
      employment_type VARCHAR(100),
      date_posted DATE,
      job_description TEXT,
      toolset_list VARCHAR(500),
      metadata_lastupdated TIMESTAMP,
      metadata_application_link VARCHAR(255),
      metadata_source VARCHAR(100),
      PRIMARY KEY(id)
	);

    ALTER TABLE IF EXISTS landing_zone_schema.clean_jobs_table OWNER to $APP_DB_USER;

    CREATE TABLE IF NOT EXISTS presentation_zone_schema.role_dim (
      role_pk VARCHAR(3),
      role VARCHAR(30),
      UNIQUE(role),
      PRIMARY KEY(role_pk)
  );

    CREATE TABLE IF NOT EXISTS presentation_zone_schema.employment_type_dim (
      employment_type_pk VARCHAR(3),
      employment_type VARCHAR(30),
      UNIQUE (employment_type),
      PRIMARY KEY(employment_type_pk)
  );
    CREATE TABLE IF NOT EXISTS presentation_zone_schema.location_dim (
      location_pk VARCHAR(5),
      country VARCHAR(30),
      city VARCHAR(30),
      PRIMARY KEY(location_pk)
  );
    CREATE TABLE IF NOT EXISTS presentation_zone_schema.seniority_dim (
      seniority_pk VARCHAR(3),
      seniority TEXT,
      UNIQUE (seniority),
      PRIMARY KEY(seniority_pk)
  );
    CREATE TABLE IF NOT EXISTS presentation_zone_schema.company_dim (
      company_pk VARCHAR(5),
      company VARCHAR(50),
      UNIQUE(company),
      PRIMARY KEY(company_pk)
  );

    CREATE TABLE IF NOT EXISTS presentation_zone_schema.date_dim (
      date_iso_format_pk date,
      date timestamp,
      num_year integer,
      num_month_in_year integer,
      num_day_in_month integer NOT NULL,
      num_day_in_week integer NOT NULL,
      is_holiday_us boolean NOT NULL,
      name_month_en text NOT NULL,
      name_day_en text NOT NULL,
      PRIMARY KEY(date_iso_format_pk)
  );
    WITH date1 AS (
	    SELECT 
		    generate_series('2023-06-01'::timestamp, '2024-02-28'::timestamp, '1 day') AS ts
    ), date2 AS (
	    SELECT
		    ts AS date, 
		    ts::DATE AS date_iso_format_pk,
        EXTRACT(YEAR FROM ts) AS num_year,
        EXTRACT(MONTH FROM ts) AS num_month_in_year,
        EXTRACT(DAY FROM ts) AS num_day_in_month,
        EXTRACT(ISODOW FROM ts) AS num_day_in_week
	    FROM date1
    ), date3 AS (
      SELECT *,
        num_month_in_year = 1 AND num_day_in_month = 1 OR 
        num_month_in_year = 7 AND num_day_in_month = 4 OR 
        num_month_in_year = 12 AND num_day_in_month = 25 AS is_holiday_us
      FROM date2
    ), date4 AS (
      SELECT
        *,
        CASE
          WHEN num_month_in_year = 1 THEN 'January'
          WHEN num_month_in_year = 2 THEN 'February'
          WHEN num_month_in_year = 3 THEN 'March'
          WHEN num_month_in_year = 4 THEN 'April'
          WHEN num_month_in_year = 5 THEN 'May'
          WHEN num_month_in_year = 6 THEN 'June'
          WHEN num_month_in_year = 7 THEN 'July'
          WHEN num_month_in_year = 8 THEN 'August'
          WHEN num_month_in_year = 9 THEN 'September'
          WHEN num_month_in_year = 10 THEN 'October'
          WHEN num_month_in_year = 11 THEN 'November'
          WHEN num_month_in_year = 12 THEN 'December'
        END AS name_month_en,
        CASE
          WHEN num_day_in_week = 1 THEN 'Monday'
          WHEN num_day_in_week = 2 THEN 'Tuesday'
          WHEN num_day_in_week = 3 THEN 'Wednesday'
          WHEN num_day_in_week = 4 THEN 'Thursday'
          WHEN num_day_in_week = 5 THEN 'Friday'
          WHEN num_day_in_week = 6 THEN 'Saturday'
          WHEN num_day_in_week = 7 THEN 'Sunday'
        END AS name_day_en
      FROM date3
    )
    INSERT INTO presentation_zone_schema.date_dim (date, date_iso_format_pk, num_year, num_month_in_year, num_day_in_month, num_day_in_week, is_holiday_us, name_month_en, name_day_en)
      SELECT
        date_iso_format_pk, 
        date,
        num_year,
        num_month_in_year,
        num_day_in_month, 
        num_day_in_week,
        is_holiday_us, 
        name_month_en, 
        name_day_en
      FROM date4;

    CREATE TABLE IF NOT EXISTS presentation_zone_schema.fact_less_table (
      date_posted_fk DATE,
      role_fk VARCHAR(3),
      company_fk VARCHAR(5),
      employment_type_fk VARCHAR(3),
      location_fk VARCHAR(5),
      toolset_list VARCHAR(500),
      days_opened INT,
      last_updated TIMESTAMP,
      PRIMARY KEY(date_posted_fk, role_fk, company_fk, employment_type_fk, location_fk),
      FOREIGN KEY(date_posted_fk) REFERENCES presentation_zone_schema.date_dim(date_iso_format_pk),
      FOREIGN KEY(role_fk) REFERENCES presentation_zone_schema.role_dim(role_pk),
      FOREIGN KEY(company_fk) REFERENCES presentation_zone_schema.company_dim(company_pk),
      FOREIGN KEY(employment_type_fk) REFERENCES presentation_zone_schema.employment_type_dim(employment_type_pk),
      FOREIGN KEY(location_fk) REFERENCES presentation_zone_schema.location_dim(location_pk)
  );
    CREATE TABLE IF NOT EXISTS presentation_zone_schema.daily_snap_ads_number (
      snap_date DATE,
      open_ads INT,
      PRIMARY KEY(snap_date)
  );
  COMMIT;
EOSQL


#GRANT CONNECT ON DATABASE $APP_DB_NAME TO $APP_DB_USER;
#GRANT ALL PRIVILEGES ON SCHEMA public TO $APP_DB_USER;
#GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO $APP_DB_USER;