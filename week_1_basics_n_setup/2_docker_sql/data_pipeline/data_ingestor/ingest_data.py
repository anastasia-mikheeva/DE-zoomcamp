#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import logging 
import time
import pandas as pd
import psycopg2
#import polars as pl
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port
    #port = 5432
    db = params.db
    table_name = params.table_name
    zones_table_name= params.zones_table_name
    logging.info(f"------------------------\nThese are the table names I have: \nDATA TABLE: {table_name}\nZONES TABLE:{zones_table_name}\n-----------------------------")
    url_data = params.url_data
    logging.info(f"------------------------\nThis is the Data URL I have: {url_data}\n-----------------------------")
    url_zones = params.url_zones 
    logging.info(f"------------------------\nThis is the Zones URL I have: {url_zones}\n-----------------------------")
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    
    # DOWNLOADING CSVs:
    csv_name = 'output.csv.gz'
    zones_csv = 'zones.csv'
    try:
        #Loading data and saving it in the container
        os.system(f"wget {url_data} -O {os.getcwd()}/{csv_name}")
        logging.info(f"Downloading and saving data as {csv_name}")
        logging.info(f"Current working directory: {os.getcwd()}")
        logging.info(f"List of things in this directory: {os.listdir()}")
        os.system
    except:
        logging.exception(f"Could not download {csv_name}")

    try:
        #Loading zones csv and saving it in the container
        os.system(f"wget {url_zones} -O {os.getcwd()}/{zones_csv}")
        logging.info(f"Downloading and saving data as {zones_csv}")
        #logging.info(f"Current working directory: {os.getcwd()}")
        logging.info(f"List of things in this directory: {os.listdir()}")
        os.system
    except:
        logging.exception(f"Could not download {zones_csv}")


    # CONNECTING TO DATABASE:
    try:
        time.sleep(10) #sometimes postgres takes a while to create the DB, this is a buffer
        logging.info(f'Trying to connect to database: {db} on port: {port}')
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
        logging.info(f'Successfully created connection to database {db} on port {port}')
    except:
        logging.exception('could not connect to database for some reason')
    # CREATING ZONES TABLE
    try:
        df_zones = pd.read_csv(zones_csv)
        logging.info("reading in the zones data")
    except:
        logging.exception("Can't read in the zones data")
    
    try:
        logging.info(f"trying to add table {zones_table_name} to the database.....")
        df_zones.head(n=0).to_sql(name=zones_table_name, con=engine, if_exists='replace')
        logging.info(f"Here is the table headers:\n{pd.io.sql.get_schema(df_zones, name=zones_table_name)}")
        df_zones.to_sql(name=zones_table_name, con=engine, if_exists='append')
        logging.info(f'added table {zones_table_name} to database successfully')
    except:
        logging.exception("something went wrong while adding the zones table")

    
    # CREATING TABLE FOR DATA
    try:
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')
        logging.info("Reading in chunks of the trips csv")
    except:
        logging.exception("Can't read in the data")
    
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    try:
        logging.info(f"trying to add table {table_name} to the database.....")
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        logging.info(f"Here is the table headers:\n{pd.io.sql.get_schema(df, name=table_name)}")
        df.to_sql(name=table_name, con=engine, if_exists='append')
        logging.info(f'added table {table_name} to database with first chunk')
    except:
        logging.exception("something went wrong while adding the table")

    while True: 

        try:
            t_start = time.time()
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time.time()

            logging.info('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            logging.exception("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--zones_table_name', required=True, help='name of the table to store zones info')
    parser.add_argument('--url_data', required=True, help='url of the csv file with the trips data')
    parser.add_argument('--url_zones', required=True, help='url of the csv file with the zones data')

    args = parser.parse_args()

    main(args)
