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


##cast members that participated in movies that won awards and made movies with at least 3 different writers

import mysql.connector
from mysql.connector import Error

def create_index(sql_command):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host="localhost",
            database="omeryosef",
            user="omeryosef",
            password="omery58087",
            port=3305
        )

        # Create a cursor object
        cursor = connection.cursor()

        # SQL command to create an index on the 'year' column of the 'year' table
        create_index_sql = sql_command

        # Execute the SQL command
        cursor.execute(create_index_sql)

        # Commit the changes to the database
        connection.commit()

        print("Index created successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

# Call the function to create the index
create_index("CREATE INDEX idx_year ON omeryosef.year(year);")
create_index("ALTER TABLE omeryosef.genres ADD FULLTEXT(genre);")





#########QUERIES

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

def query_4(): ##need to add FULL-TEXT index for this to work
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
                select og.imdbID
                from omeryosef.genres og
                where og.genre like '%Fantasy%'
                union all
                SELECT op.imdbID
                FROM omeryosef.plot op
                WHERE MATCH(op.fullplot) AGAINST('magic')
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

def query_5(): ##we added FULL-TEXT index on genre and now the query is uper fast (good cause
    ##genre is used both in join and group by hence the index is very useful)
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
            WITH avg_ratings AS (
                SELECT og.genre
                      ,AVG(orr.imdbRating) AS avg_rating
                FROM omeryosef.genres og
                JOIN omeryosef.rating orr ON orr.imdbID = og.imdbID
                GROUP BY og.genre
            ), rated_movies AS (
                SELECT orr.imdbID
                      ,orr.imdbRating
                      ,og.genre
                      ,oc.director
                FROM omeryosef.rating orr
                JOIN omeryosef.genres og ON og.imdbID = orr.imdbID
                JOIN omeryosef.crew oc ON oc.imdbID = orr.imdbID
            )
            SELECT director
                  ,COUNT(DISTINCT imdbID) AS movie_count
            FROM rated_movies rm
            JOIN avg_ratings ar ON rm.genre = ar.genre
            WHERE rm.imdbRating > ar.avg_rating
            and director is not null
            GROUP BY director
            HAVING COUNT(DISTINCT imdbID) > 5;
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


WITH RECURSIVE years AS (
  SELECT 2010 as year -- Starting year
  UNION ALL
  SELECT year + 1 FROM years WHERE year < YEAR(CURDATE()) -- Ending with the current year
),
movies AS (
  SELECT
    oy.year,
    COUNT(DISTINCT oc.imdbID) AS movies_counter,
    AVG(orr.imdbRating) AS avg_Rating_per_year
  FROM omeryosef.year oy
  LEFT JOIN omeryosef.crew oc ON oy.imdbID = oc.imdbID AND oc.cast LIKE '%Will Smith%'
  LEFT JOIN omeryosef.rating orr ON orr.imdbID = oc.imdbID
  WHERE oy.year >= 2014
  GROUP BY oy.year
)
SELECT
  y.year,
  COALESCE(m.movies_counter, 0) AS movies_counter,
  COALESCE(m.avg_Rating_per_year, "No Rating") AS avg_Rating_per_year
FROM years y
LEFT JOIN movies m ON y.year = m.year
ORDER BY y.year;
