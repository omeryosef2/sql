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


def create_all_tables():
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


def create_all_index():
    create_index("CREATE INDEX idx_year ON omeryosef.year(year) USING BTREE;")
    create_index("ALTER TABLE omeryosef.genres ADD FULLTEXT(genre);")
    create_index("ALTER TABLE omeryosef.plot ADD FULLTEXT(fullplot);")