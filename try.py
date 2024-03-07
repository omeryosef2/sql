import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error

def create_connection():
    try:
        connection_params = {
            "host": "localhost",
            "database": "omeryosef",
            "user": "omeryosef",
            "password": "omery58087",
            "port": 3305,
        }
        print(type(connection_params))
        cnx = mysql.connector.connect(**connection_params)
        if cnx.is_connected():
            print("Successfully connected to the database")
            cursor = cnx.cursor()
            return cnx, cursor  # Return both the connection and cursor
        else:
            print("Connection was unsuccessful")
            return None, None
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None, None


def query_1(genre=None):
    cnx, cursor = create_connection()  # Unpack the returned tuple

    if cnx is None or cursor is None:
        print("Failed to connect to the database.")
        return None

    if genre is None:
        genre = input("Put here: ")  # This will prompt the user to input the genre
    if len(genre) == 0:
        genre = "Comedy"
        print("You didn't choose your genre, so I will go with Comedy :)")

    query = """
    SELECT og.imdbID AS movieID, og.genre, ot.title
    FROM omeryosef.genres og
    INNER JOIN omeryosef.title ot ON ot.imdbId = og.imdbID 
    WHERE MATCH(og.genre) AGAINST(%s IN BOOLEAN MODE)
    """
    try:
        cursor.execute(query, (genre,))  # Use parameterized query for safety
        results = cursor.fetchall()
    except Error as e:
        print(f"Error executing query: {e}")
        results = None

    # Close the cursor and connection
    cursor.close()
    cnx.close()

    return results

print(query_1("Comedy"))