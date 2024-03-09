import pandas as pd
import numpy as np
from mysql.connector import connect, Error
from tqdm import tqdm

from create_db_script import create_table, create_index

zip_path = "./IMDB.zip"
csv_file_name = "IMDB.csv"

df = pd.read_csv(zip_path, compression="zip")

df.drop_duplicates(inplace=True)
df["imdbVotes"] = df["imdbVotes"].fillna(0)
df = df.loc[df.groupby("imdbID")["imdbVotes"].idxmax()].reset_index(drop=True)
df = df.replace({np.nan: None})
df["rating"] = df["rating"].replace("Not Rated", None)


def insert_to_table(df, insert_query, column_names):
    try:
        connection = connect(
            host='localhost',
            database='omeryosef',
            user='omeryosef',
            password='omery58087',
            port=3305
        )
        cursor = connection.cursor()

        # Loop through the dataframe
        for _, row in tqdm(df.iterrows(), total=df.shape[0]):
            # Extract data for each column specified in `column_names`
            data_tuple = tuple(row[col] for col in column_names)
            try:
                cursor.execute(insert_query, data_tuple)
                connection.commit()
            except Error as e:
                print(f"Error inserting data: {e}")
                connection.rollback()

    except Error as e:
        print(f"Error: '{e}'")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def insert_all_tables_data():
    insert_query1 = """
    INSERT INTO year (imdbID, year) VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE year=VALUES(year);
    """
    column_names1 = ['imdbID','year']
    insert_to_table(df, insert_query1, column_names1)

    insert_query2 = """
    INSERT INTO plot (imdbID, fullplot) VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE fullplot=VALUES(fullplot);
    """
    column_names2 = ['imdbID', 'fullplot']
    insert_to_table(df, insert_query2, column_names2)

    insert_query3 = """
    INSERT INTO title (imdbID, title) VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE title=VALUES(title);
    """
    column_names3 = ['imdbID', 'title']
    insert_to_table(df, insert_query3, column_names3)

    insert_query4 = """
    INSERT INTO crew (imdbID, director, writer ,cast) VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE director=VALUES(director), writer=VALUES(writer), cast=VALUES(cast);
    """
    column_names4 = ['imdbID', 'director', 'writer', 'cast']
    insert_to_table(df, insert_query4, column_names4)

    insert_query5 = """
    INSERT INTO genres (imdbID, genre) VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE genre=VALUES(genre);
    """
    column_names5 = ['imdbID', 'genre']
    insert_to_table(df, insert_query5, column_names5)

    insert_query6 = """
    INSERT INTO rating (imdbID, imdbRating, awards) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE imdbRating=VALUES(imdbRating), awards=VALUES(awards);
    """
    column_names6 = ['imdbID', 'imdbRating', 'awards']
    insert_to_table(df, insert_query6, column_names6)


