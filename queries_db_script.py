import pandas as pd
import numpy as np
import mysql
import mysql.connector
from mysql.connector import Error
pd.set_option('display.max_columns', None)

def create_connection():
    connection_params = {
        "host": "localhost",
        "database": "omeryosef",
        "user": "omeryosef",
        "password": "omery58087",
        "port": 3305
    }
    cnx = mysql.connector.connect(**connection_params)
    return cnx


def query_1(genre=None):
    """
    Returns movies by their genre.

    An index on the genre column can significantly improve query performance,
    especially for full-text search queries like this.
    """
    try:
        cnx = create_connection()
        cursor = cnx.cursor()

        if genre is None:
            genre = input("Enter genre: ").strip().lower()  # This will prompt the user to input the genre
            if not genre:
                genre = "Comedy"
                print("You didn't choose a genre, so I will go with Comedy :)")

        # Use parameterized query to prevent SQL injection
        query = """
        SELECT og.imdbID AS movieID,
               og.genre,
               ot.title
        FROM omeryosef.genres og
        INNER JOIN omeryosef.title ot ON ot.imdbId = og.imdbID
        WHERE MATCH(og.genre) AGAINST(%s IN BOOLEAN MODE)
        """
        cursor.execute(query, (genre.lower(),))

        # Fetch the results
        results = cursor.fetchall()

        # Create DataFrame with specified column names
        df = pd.DataFrame(results, columns=['MovieID', 'Genre', 'Title'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error
    finally:
        # Clean up
        if cursor is not None:
            cursor.close()
        if cnx is not None and cnx.is_connected():
            cnx.close()


def query_2(year1=None, year2=None):
    try:
        cnx = create_connection()
        cursor = cnx.cursor()

        if year1 is None:
            year1 = input("Enter first year of the range in YYYY format: ")
        if year2 is None:
            year2 = input("Enter second year of the range in YYYY format: ")

        # Defaulting years if not provided or incorrectly provided
        if len(str(year1)) != 4:
            year1 = "1990"
            print("You didn't choose the first year of the range, so I will go with 1990 :)")
        if len(str(year2)) != 4:
            year2 = "2010"
            print("You didn't choose the second year of the range, so I will go with 2010 :)")

        if year1 > year2:
            year1, year2 = "1990", "2010"  # Swap to default values if the range is incorrect
            print("We got you, malicious user!")

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

        # Create DataFrame with specified column names
        df = pd.DataFrame(results, columns=['MovieID', 'Title', 'Year', 'IMDbRating', 'Awards'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()

def query_3(num_of_movies=None):
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if num_of_movies is None:
            num_of_movies = input("Enter the min number of movies per year: ")
            try:
                num_of_movies = int(num_of_movies)
            except ValueError:
                num_of_movies = 100
                print("You can't fool me")

        # Prepare the query
        query = """
                SELECT og.genre, 
                       oy.year,
                       COUNT(DISTINCT oy.imdbID) AS movies_number
                FROM omeryosef.year oy
                JOIN omeryosef.genres og ON og.imdbID = oy.imdbID
                WHERE og.genre IS NOT NULL
                GROUP BY og.genre, oy.year
                HAVING COUNT(DISTINCT oy.imdbID) > %s
        """
        # Execute the query with parameters
        cursor.execute(query, (num_of_movies,))

        # Fetch the results
        results = cursor.fetchall()

        # Create DataFrame with specified column names
        df = pd.DataFrame(results, columns=['Genre', 'Year', 'Movies Number'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()


def query_4(word=None):
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if word is None:
            word = input("Enter the word you want to appear in the plot: ")

        # Prepare the query
        query = """
                SELECT og.imdbID, ot.title
                FROM omeryosef.genres og
                JOIN omeryosef.title ot ON ot.imdbID = og.imdbID
                WHERE og.genre LIKE '%Fantasy%'
                UNION ALL
                SELECT op.imdbID, ot.title
                FROM omeryosef.plot op
                JOIN omeryosef.title ot ON ot.imdbID = op.imdbID
                WHERE MATCH(op.fullplot) AGAINST(%s IN BOOLEAN MODE)
                """
        # Execute the query with parameters
        cursor.execute(query, (word.lower(),))

        # Fetch the results
        results = cursor.fetchall()

        # Create DataFrame with specified column names
        df = pd.DataFrame(results, columns=['IMDBid', 'Title'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()

def query_5(num_movies_above_avg = None): ##we added FULL-TEXT index on genre and now the query is uper fast (good cause
    ##genre is used both in join and group by hence the index is very useful)
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if num_movies_above_avg == None:
            num_movies_above_avg = input("Enter min number of movies above the average rating per director: ")

        if type(num_movies_above_avg) != int:
            num_movies_above_avg = 5
            print("You tried to fool me")
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
            HAVING COUNT(DISTINCT imdbID) > %s;
        """
        # Execute the query with parameters
        cursor.execute(query, (num_movies_above_avg,))
        # Fetch the results
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['director', 'movie_count'])
        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and cnx.is_connected():
            cnx.close()


def query_6(actor=None, start_year=None, end_year=None):
    try:
        cnx = create_connection()
        cursor = cnx.cursor()

        # Default actor input if none provided
        if actor is None:
            actor = input("Enter your favorite actor: ").strip()

        # Convert actor input to lowercase for case-insensitive search
        actor = actor.lower()
        actor_like_pattern = "%" + actor.replace('%', '%%').replace('_', '\\_') + "%"

        # Prompt for start and end year if not provided
        if start_year is None:
            start_year = input("Enter the start year: ").strip()

        if end_year is None:
            end_year = input("Enter the end year: ").strip()

        query = """
            WITH RECURSIVE years AS (
              SELECT %s AS year
              UNION ALL
              SELECT year + 1 FROM years WHERE year < %s
            ),
            movies AS (
              SELECT
                y.year,
                COUNT(DISTINCT oc.imdbID) AS movies_counter,
                AVG(orr.imdbRating) AS avg_Rating_per_year
              FROM years y
              LEFT JOIN omeryosef.crew oc ON oc.cast LIKE %s
              LEFT JOIN omeryosef.rating orr ON orr.imdbID = oc.imdbID
              WHERE y.year BETWEEN %s AND %s
              GROUP BY y.year
            )
            SELECT
              y.year,
              COALESCE(m.movies_counter, 0) AS movies_counter,
              COALESCE(m.avg_Rating_per_year, 'No Rating') AS avg_Rating_per_year
            FROM years y
            LEFT JOIN movies m ON y.year = m.year
            ORDER BY y.year;
        """

        cursor.execute(query, (start_year, end_year, actor_like_pattern, start_year, end_year))

        # Fetch the results
        results = cursor.fetchall()

        # Create DataFrame with correct column names
        df = pd.DataFrame(results, columns=['Year', 'Movies Counter', 'Average Rating per Year'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if cursor is not None:
            cursor.close()
        if cnx is not None and cnx.is_connected():
            cnx.close()


def query_7(genre = None, year = None):
    try:
        cnx = create_connection()
        cursor = cnx.cursor()

        if genre is None:
            genre = input("Enter genre: ").strip()  # This will prompt the user to input the genre
            if not genre:
                genre = "Comedy"
                print("You didn't choose a genre, so I will go with Comedy :)")

        if year is None:
            year = input("Enter year: ").strip()
            if not year:
                year = 2010
            else:
                year = int(year)

        query = """
            select oy.year, oc.director, og.genre
                ,count(distinct oc.imdbID) as movies_counter
            from omeryosef.genres og
            join omeryosef.crew oc
            on oc.imdbID = og.imdbID
            join omeryosef.year oy
            on oc.imdbID = oy.imdbID
            WHERE MATCH(og.genre) AGAINST(%s IN BOOLEAN MODE)
                and oy.year > %s
                and oc.director is not null
            group by oy.year, oc.director, og.genre
            having count(distinct oc.imdbID) > 1
                    """

        cursor.execute(query, (genre, year))

        # Fetch the results
        results = cursor.fetchall()

        # Create DataFrame with correct column names
        df = pd.DataFrame(results, columns=['Year', 'Director', 'Genre','Movies_Counter'])

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if cursor is not None:
            cursor.close()
        if cnx is not None and cnx.is_connected():
            cnx.close()


