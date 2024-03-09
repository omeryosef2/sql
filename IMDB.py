import pandas as pd
import numpy as np
import mysql
import mysql.connector


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
create_index("ALTER TABLE omeryosef.plot ADD FULLTEXT(fullplot);")




#########QUERIES




