# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
#from itemadapter import ItemAdapter
import psycopg2
from dotenv import load_dotenv
import os
import time
import logging

class PostgreSQLPipeline:
    def __init__(self, max_retries=3, delay_seconds=10):
        self.max_retries = max_retries
        self.delay_seconds = delay_seconds
        logging.info('Init Connection to PostgresDB...')
        self.connection = self.create_retry_connection()
        self.cursor = self.connection.cursor()
        self.truncate_table()
    def create_retry_connection(self):
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                POSTGRES_USER=os.environ['POSTGRES_USER']
                POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
                connection = psycopg2.connect(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresdb_container:5432/jobs_warehouse')
                connection.autocommit = True
                logging.info('Connected to PostgresDB Cluster.')
                if connection:
                    connection.set_session(autocommit=True)
                    return connection
            except Exception as e:
                logging.error(f"Connection failed. Caught ERROR {e} | Retrying ({retry_count + 1}/{self.max_retries})...")
                retry_count += 1
                time.sleep(self.delay_seconds)
        raise

    def truncate_table(self):
        truncate_sql = '''
        TRUNCATE TABLE landing_zone_schema.raw_jobs_table;
        TRUNCATE TABLE landing_zone_schema.clean_jobs_table;
        '''
        self.cursor.execute(truncate_sql)
        self.connection.commit()


    def process_item(self, item, spider):
        self.cursor.execute('''
        INSERT INTO landing_zone_schema.raw_jobs_table (
            title, company, location, employment_type, date_posted,
            job_description, metadata_lastupdated, metadata_source, metadata_application_link
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        ''', (
            item['title'],
            item['company'],
            item['location'],
            item['employment_type'],
            item['date_posted'],
            item['description'],
            item['metadata_lastupdated'],
            item['metadata_source'],
            item['metadata_application_link']
        ))
        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        logging.info("Closing Connection")
        self.connection.close()
