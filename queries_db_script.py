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


def query_2(year1 = None, year2 = None):
    ##explain why we have index on year column
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if year1 == None:
            year1 = input("Enter first year of the range in YYYY format: ")
        if year2 == None:
            year2 = input("Enter second year of the range in YYYY format: ")

        # Defaulting years if not provided
        if len(year1) == 0:
            year1 = "1990"
            print("You didn't choose the first year of the range, so I will go with 1990 :)")
        if len(year2) == 0:
            year2 = "2010"
            print("You didn't choose the second year of the range, so I will go with 2010 :)")

        if year1 > year2:
            year1 = "1990"
            year2 = "2010"
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
        print(results)
        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and cnx.is_connected():
            cnx.close()

def query_3(num_of_movies = None):
    ##we have index on genre and year - explain why it helps
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if num_of_movies == None:
            num_of_movies = input("Enter the min number of movies per year: ")

        if type(num_of_movies) != int:
            num_of_movies = 100
            print("You can't fool me")
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
                having count(distinct oy.imdbID) > %s
        """
        # Execute the query with parameters
        cursor.execute(query, num_of_movies)

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
        if 'connection' in locals() and cnx.is_connected():
            cnx.close()

def query_4(word = None): ##need to add FULL-TEXT index for this to work
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if word == None:
            word = input("Enter the word you want to appear in the plot: ")

        # Prepare the query
        query = """
                select og.imdbID
                from omeryosef.genres og
                where og.genre like '%Fantasy%'
                union all
                SELECT op.imdbID
                FROM omeryosef.plot op
                WHERE MATCH(op.fullplot) AGAINST('{word}')
        """
        # Execute the query with parameters
        cursor.execute(query, (f'%{word}%',))

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
        if 'connection' in locals() and cnx.is_connected():
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
        cursor.execute(query, num_movies_above_avg)

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
        if 'connection' in locals() and cnx.is_connected():
            cnx.close()

def query_6(actor = None): ##we added FULL-TEXT index on genre and now the query is uper fast (good cause
    ##genre is used both in join and group by hence the index is very useful)
    try:
        # Establish connection
        cnx = create_connection()
        cursor = cnx.cursor()

        if actor == None:
            actor = input("Enter your favorite actor: ")

        if actor.isalpha() == False:
            actor = "will smith"
            print("An actor has to be an alphabetic string")
        # Prepare the query
        query = """
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
              LEFT JOIN omeryosef.crew oc ON oy.imdbID = oc.imdbID AND oc.cast LIKE '%{actor}%'
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
        """
        # Execute the query with parameters
        cursor.execute(query, (f'%{actor}%',))

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
        if 'connection' in locals() and cnx.is_connected():
            cnx.close()
