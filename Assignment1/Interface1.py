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
    delta = numberofpartitions / 5.0
    lower_range = 0
    partitionNumber = 0
    with openconnection.cursor() as cur:
        while partitionNumber < numberofpartitions:
            if lower_range == 0:
                cur.execute(f"""
                    CREATE TABLE range_ratings_part{partitionNumber} AS
                    SELECT * FROM {ratingstablename} 
                    WHERE rating >= {lower_range} AND rating <= {lower_range+delta}
                """)
            else:
                cur.execute(f"""
                    CREATE TABLE range_ratings_part{partitionNumber} AS
                    SELECT * FROM {ratingstablename} 
                    WHERE rating > {lower_range} AND rating <= {lower_range+delta}
                """) 
            lower_range = lower_range + delta
            partitionNumber += 1
    openconnection.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as cur:
        for partition in range(numberofpartitions):
            cur.execute(f"""
                CREATE TABLE round_robin_ratings_part{partition} AS
                SELECT userid, movieid, rating FROM
                (
                    SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowNumber
                    FROM {ratingstablename}
                ) AS TEMP
                WHERE MOD(TEMP.rowNumber-1, 5) = {partition}
            """)
    openconnection.commit()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    rowCount = insertAndCount(ratingstablename, userid, itemid, rating, openconnection)
    partitionCount = getPartitionCount("round_robin_ratings_part", openconnection)
    partition = (rowCount-1) % partitionCount
    with openconnection.cursor() as cur:
        cur.execute(f"""
            INSERT INTO round_robin_ratings_part{partition}
            VALUES ({userid}, {itemid}, {rating})
        """)
    openconnection.commit()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    partitionCount = getPartitionCount("range_ratings_part", openconnection)
    with openconnection.cursor() as cur:
        cur.execute(f"""
            INSERT INTO range_ratings_part{partition}
            VALUES ({userid}, {itemid}, {rating})
        """)
    openconnection.commit()

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass #Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass # Remove this once you are done with implementation

def insertAndCount(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {ratingstablename} 
            VALUES ({userid}, {itemid}, {rating})
        """)
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {ratingstablename}
        """)
        rowCount = int(cur.fetchone()[0])
    return rowCount

def getPartitionCount(tablePrefix, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name LIKE '{tablePrefix}%'
            AND table_schema = 'public'
        """)
        partitionCount = int(cur.fetchone()[0])
    return partitionCount

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
