import pandas as pd
import numpy as np
from tqdm import tqdm

zip_path = './IMDB.zip'
csv_file_name = 'IMDB.csv'  # Optional if the ZIP contains only one CSV file

# If the ZIP contains only one CSV file, you can omit the `csv_file_name`
df = pd.read_csv(zip_path, compression='zip')

df.drop_duplicates(inplace=True)
df['imdbVotes'] = df['imdbVotes'].fillna(0)
df = df.loc[df.groupby('imdbID')['imdbVotes'].idxmax()].reset_index(drop=True)

import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(
        host='localhost',
        database='omeryosef',
        user='omeryosef',
        password='omery58087',
        port = 3305
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

insert_query = """
INSERT INTO movies (imdbID, rating, awards) VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE rating=VALUES(rating), awards=VALUES(awards);
"""

# Ensure the cursor and connection are still open here
for _, row in df.iterrows():
    # Convert rating to float to ensure proper type handling
    data_tuple = (row['imdbID'], float(row['rating']), row['awards'])
    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, data_tuple)
        connection.commit()
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()

insert_query = """
INSERT INTO movies (imdbID, rating, awards) VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE rating=VALUES(rating), awards=VALUES(awards);
"""

# Ensure the cursor and connection are still open here
for _, row in df.iterrows():
    # Convert rating to float to ensure proper type handling
    data_tuple = (row['imdbID'], float(row['rating']), row['awards'])
    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, data_tuple)
        connection.commit()
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()





