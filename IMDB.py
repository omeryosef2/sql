import pandas as pd
import numpy as np
from tqdm import tqdm
import mysql

zip_path = "./IMDB.zip"
csv_file_name = "IMDB.csv"  # Optional if the ZIP contains only one CSV file

# If the ZIP contains only one CSV file, you can omit the `csv_file_name`
df = pd.read_csv(zip_path, compression="zip")

df.drop_duplicates(inplace=True)
df["imdbVotes"] = df["imdbVotes"].fillna(0)
df = df.loc[df.groupby("imdbID")["imdbVotes"].idxmax()].reset_index(drop=True)

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(
        host="localhost",
        database="omeryosef",
        user="omeryosef",
        password="omery58087",
        port=3305,
    )
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)

df = df.replace({np.nan: None})
df["rating"] = df["rating"].replace("Not Rated", None)
# insert_query = "INSERT INTO genres (imdbID, genre) VALUES (%s, %s) ON DUPLICATE KEY UPDATE genre=VALUES(genre);"
#
# for _, row in tqdm(df.iterrows(),total = df.shape[0]):
#     try:
#         # Ensure that the cursor is still defined and the connection is open
#         cursor.execute(insert_query, (row['imdbID'], row['genre']))
#         connection.commit()
#     except mysql.connector.Error as e:
#         print(f"Error: '{e}'")
#         connection.rollback()

# create_table_query = """
# CREATE TABLE IF NOT EXISTS year (
#     imdbID BIGINT PRIMARY KEY,
#     year INT
# );
# """
# import mysql.connector
# from mysql.connector import Error
#
# try:
#     connection = mysql.connector.connect(
#         host='localhost',
#         database='omeryosef',
#         user='omeryosef',
#         password='omery58087',
#         port = 3305
#     )
#     if connection.is_connected():
#         cursor = connection.cursor()
#         # Adjusted to use FLOAT for the rating
#         cursor.execute(create_table_query)
#         print("Table 'movies' created successfully.")
# except Error as e:
#     print(f"Error: '{e}'")
# finally:
#     if connection.is_connected():
#         cursor.close()

#
# insert_query = """
# INSERT INTO year (imdbID, year) VALUES (%s, %s)
# ON DUPLICATE KEY UPDATE year=VALUES(year);
# """
#
# # Ensure the cursor and connection are still open here
# for _, row in tqdm(df.iterrows(), total=df.shape[0]):
#     # Convert rating to float to ensure proper type handling
#     data_tuple = (row["imdbID"], row["year"])
#     try:
#         cursor = connection.cursor()
#         cursor.execute(insert_query, data_tuple)
#         connection.commit()
#     except Error as e:
#         print(f"Error inserting data: {e}")
#         connection.rollback()

# insert_query = """
# INSERT INTO plot (imdbID, plot, fullplot) VALUES (%s, %s, %s)
# ON DUPLICATE KEY UPDATE plot=VALUES(plot), fullplot=VALUES(fullplot);
# """
#
# # Ensure the cursor and connection are still open here
# for _, row in df.iterrows():
#     # Convert rating to float to ensure proper type handling
#     data_tuple = (row["imdbID"], row["plot"], row["fullplot"])
#     try:
#         cursor = connection.cursor()
#         cursor.execute(insert_query, data_tuple)
#         connection.commit()
#     except Error as e:
#         print(f"Error inserting data: {e}")
#         connection.rollback()
# #
# insert_query = """
# INSERT INTO country (imdbID, country) VALUES (%s, %s)
# ON DUPLICATE KEY UPDATE country=VALUES(country);
# """
#
# # Ensure the cursor and connection are still open here
# for _, row in df.iterrows():
#     # Convert rating to float to ensure proper type handling
#     data_tuple = (row["imdbID"], row["country"])
#     try:
#         cursor = connection.cursor()
#         cursor.execute(insert_query, data_tuple)
#         connection.commit()
#     except Error as e:
#         print(f"Error inserting data: {e}")
#         connection.rollback()
# #
# insert_query = """
# INSERT INTO crew (imdbID, director, writer ,cast) VALUES (%s, %s, %s, %s)
# ON DUPLICATE KEY UPDATE director=VALUES(director), writer=VALUES(writer), cast=VALUES(cast);
# """
#
# # Ensure the cursor and connection are still open here
# for _, row in df.iterrows():
#     # Convert rating to float to ensure proper type handling
#     data_tuple = (row["imdbID"], row["director"], row["writer"], row["cast"])
#     try:
#         cursor = connection.cursor()
#         cursor.execute(insert_query, data_tuple)
#         connection.commit()
#     except Error as e:
#         print(f"Error inserting data: {e}")
#         connection.rollback()


##take all fantasy movies with a certain minimum rating union movies from other genres that contain certain words in the plot
##all the directors that made at least 5 movies in a certain genre with ratings that are above the average per genre
##cast members that participated in movies that won awards and made movies with at least 3 different writers
##count movies per genre and and year -- simple query

def query_1(): ##return movies by their genre
    connection = mysql.connector.connect(
        host="localhost",
        database="omeryosef",
        user="omeryosef",
        password="omery58087",
        port=3305
    )
    cnx = mysql.connector.connect(**connection)
    cursor = cnx.cursor()
    genre = input("Put here: ")  # This will prompt the user to input the genre
    if len(genre) == 0:
        genre = "Comedy"
        print("You didn't choose your genre, so I will go with Comedy :)")
    query = f"""
    SELECT og.imdbID AS movieID
           ,og.genre
           ,ot.title
    FROM omeryosef.genres og
    INNER JOIN omeryosef.title ot ON ot.imdbId = og.imdbID 
    WHERE og.genre LIKE '%{genre}%'
    """
    cursor.execute(query, (f'%{genre}%',))

    # Fetch the results
    results = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    cnx.close()

    # Return or process the results as needed
    return results


import mysql.connector

import mysql.connector

def query_2():
    try:
        # Establish connection
        connection = mysql.connector.connect(
            host="localhost",
            database="omeryosef",
            user="omeryosef",
            password="omery58087",
            port=3305
        )
        cursor = connection.cursor()

        # Input from user
        year1 = input("Enter first year of the range in YYYY format: ")
        year2 = input("Enter second year of the range in YYYY format: ")

        # Defaulting years if not provided
        if len(year1) == 0:
            year1 = "1990"
            print("You didn't choose the first year of the range, so I will go with 1990 :)")
        if len(year2) == 0:
            year2 = "2010"
            print("You didn't choose the second year of the range, so I will go with 2010 :)")

        # Prepare the query
        query = """
                SELECT ot.imdbID AS movieID,
                       ot.title,
                       oy.year,
                       orr.imdbRating,
                       orr.awards
                FROM omeryosef.title ot
                JOIN omeryosef.year oy ON oy.imdbID = ot.imdbID
                JOIN omeryosef.rating orr ON orr.imdbID = ot.imdbID
                WHERE oy.year BETWEEN %s AND %s
        """
        # Execute the query with parameters
        cursor.execute(query, (year1, year2))

        # Fetch the results
        results = cursor.fetchall()
        print(results)
        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

def query_3():
    try:
        # Establish connection
        connection = mysql.connector.connect(
            host="localhost",
            database="omeryosef",
            user="omeryosef",
            password="omery58087",
            port=3305
        )
        cursor = connection.cursor()

        # Prepare the query
        query = """
                select og.genre, 
                        oy.year,
                        count(distinct oy.imdbID) as movies_number
                from omeryosef.year oy
                join omeryosef.genres og
                on og.imdbID = oy.imdbID
                where og.genre is not null
                group by og.genre, oy.year
                having count(distinct oy.imdbID) > 100
        """
        # Execute the query with parameters
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()
        print(results)
        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()






