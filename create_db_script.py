import pandas as pd
import numpy as np
from tqdm import tqdm
import mysql
import mysql.connector
import mysql.connector
from mysql.connector import Error


def create_table(query_for_creation):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='omeryosef',
            user='omeryosef',
            password='omery58087',
            port = 3305
        )
        if connection.is_connected():
            cursor = connection.cursor()
            # Adjusted to use FLOAT for the rating
            cursor.execute(query_for_creation)
            print("Table created successfully.")
    except Error as e:
        print(f"Error: '{e}'")
    finally:
        if connection.is_connected():
            cursor.close()

create_table_query1 = """
CREATE TABLE IF NOT EXISTS year1 (
    imdbID BIGINT PRIMARY KEY,
    year INT
);
"""
create_table(create_table_query1)

create_table_query2 = """
CREATE TABLE IF NOT EXISTS title (
    imdbID BIGINT PRIMARY KEY,
    title TEXT
);
"""
create_table(create_table_query2)

create_table_query3 = """
CREATE TABLE IF NOT EXISTS rating (
    imdbID BIGINT PRIMARY KEY,
    rating FLOAT,
    awards TEXT
);
"""
create_table(create_table_query3)

create_table_query4 = """
CREATE TABLE IF NOT EXISTS plot (
    imdbID BIGINT PRIMARY KEY,
    plot TEXT,
    fullplot TEXT
);
"""
create_table(create_table_query4)

create_table_query5 = """
CREATE TABLE IF NOT EXISTS genres (
    imdbID BIGINT PRIMARY KEY,
    genre TEXT
);
"""
create_table(create_table_query5)

create_table_query6 = """
CREATE TABLE IF NOT EXISTS crew (
    imdbID BIGINT PRIMARY KEY,
    director TEXT,
    writer TEXT,
    cast TEXT
);
"""
create_table(create_table_query6)

create_table_query7 = """
CREATE TABLE IF NOT EXISTS country (
   imdbID BIGINT PRIMARY KEY,
   country TEXT
);
"""
create_table(create_table_query7)