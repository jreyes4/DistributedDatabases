import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE {ratingstablename}(
                userid integer,
                temp1 text,
                movieid integer,
                temp2 text,
                rating float,
                temp3 text,
                temp4 text
            )
        """)
        with open(ratingsfilepath, 'r') as f:
            cur.copy_from(f, ratingstablename, sep=':')
        cur.execute(f"""
            ALTER TABLE {ratingstablename}
            DROP COLUMN temp1,
            DROP COLUMN temp2,
            DROP COLUMN temp3,
            DROP COLUMN temp4
        """)
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    pass # Remove this once you are done with implementation
    MAX_RATING = 5
    upper_range = numberofpartitions / MAX_RATING
    lower_range = 0
    partitionNumber = 0
    with openconnection.cursor() as cur:
        while lower_range <= MAX_RATING:
            if lower_range == 0:
                cur.execute(f"""
                    CREATE TABLE RangeRatingsPart{partitionNumber} AS
                    SELECT * FROM {ratingstablename} 
                    WHERE rating >= {lower_range} AND rating <= {lower_range+upper_range}
                """)
            else:
                cur.execute(f"""
                    CREATE TABLE RangeRatingsPart{partitionNumber} AS
                    SELECT * FROM {ratingstablename} 
                    WHERE rating > {lower_range} AND rating <= {lower_range+upper_range}
                """) 
            lower_range = lower_range + upper_range
            partitionNumber += 1


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass # Remove this once you are done with implementation


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass #Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass # Remove this once you are done with implementation


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
