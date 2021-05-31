#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    with openconnection.cursor() as c:
        c.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
        Minimum = c.fetchone()
        min_value = float(Minimum[0])

        c.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
        Maximum = c.fetchone()
        max_value = float(Maximum[0])

        interval = (max_value - min_value) / 5
        # print(interval)
        thread = [0, 0, 0, 0, 0]
        for i in range(5):
            c.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            openconnection.commit()
            c.execute("CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + InputTable + " WHERE 0=1 ")
            openconnection.commit()
        for i in range(5):
            if i == 0:
                lower_Bound = min_value
                upper_Bound = min_value + interval
            else:
                lower_Bound = upper_Bound
                upper_Bound = upper_Bound + interval

            thread[i] = threading.Thread(target=range_sorted, args=(
                InputTable, SortingColumnName, i, lower_Bound, upper_Bound, openconnection))

            thread[i].start()

        for j in range(5):
            thread[j].join()
        c.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        openconnection.commit()
        c.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 0=1 ")
        openconnection.commit()
        for i in range(5):
            c.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + "")
            openconnection.commit()
            c.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            openconnection.commit()



def range_sorted(InputTable, SortingColumnName, table_index, minm_value, maxm_value, openconnection):
    c = openconnection.cursor()

    table_name = "range_part" + str(table_index)
    if table_index == 0:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(minm_value) + " AND " + SortingColumnName + " <= " + str(maxm_value) + " ORDER BY " + SortingColumnName + " ASC"
    else:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(
            minm_value) + " AND " + SortingColumnName + " <= " + str(
            maxm_value) + " ORDER BY " + SortingColumnName + " ASC"

    c.execute(query)
    openconnection.commit()

def parallelexecuteQuery(InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,i, openconnection,lower_range,upper_range):
    c = openconnection.cursor()
    if i == 0:
        c.execute("CREATE TABLE partitionTable1" + str(i) + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(lower_range) + ") AND (" + Table1JoinColumn + " <= " + str(upper_range) + ")")
        openconnection.commit()
        c.execute("CREATE TABLE partitionTable2" + str(i) + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(lower_range) + ") AND (" + Table2JoinColumn + " <= " + str(upper_range) + ")")
        openconnection.commit()
    else:
        c.execute("CREATE TABLE partitionTable1" + str(i) + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(lower_range) + ") AND (" + Table1JoinColumn + " <= " + str(upper_range) + ")")
        openconnection.commit()
        c.execute("CREATE TABLE partitionTable2" + str(i) + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(lower_range) + ") AND (" + Table2JoinColumn + " <= " + str(upper_range) + ")")
        openconnection.commit()
    c.execute("CREATE TABLE OutputTable" + str(i) + " AS SELECT * FROM " + InputTable1 + "," + InputTable2 + " WHERE 0=1")
    openconnection.commit()
    query = "INSERT INTO OutputTable" + str(i) + "  SELECT * FROM partitionTable1" + str(i) + " INNER JOIN partitionTable2" + str(i) + " ON partitionTable1" + str(i) + "." + Table1JoinColumn + " = partitionTable2" + str(i) + "." + Table2JoinColumn +""
    c.execute(query)
    openconnection.commit()



def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Implement ParallelJoin Here.

    with openconnection.cursor() as c:
        c.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        Minimum1 = c.fetchone()
        min_value1 = float(Minimum1[0])

        c.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        Maximum1 = c.fetchone()
        max_value1 = float(Maximum1[0])

        c.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        Minimum2 = c.fetchone()
        min_value2 = float(Minimum2[0])

        c.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        Maximum2 = c.fetchone()
        max_value2 = float(Maximum2[0])

        min_value=min(min_value1,min_value2)
        max_value=max(max_value1,max_value2)

        interval=(max_value-min_value)/5
        # print(min_value,max_value)
        thread = [0,0,0,0,0]
        c.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable1 + "," + InputTable2 + " WHERE 0=1")
        openconnection.commit()
        for i in range(5):
            lower_range = min_value+i*interval
            upper_range = lower_range+interval
            thread[i]= threading.Thread(target=parallelexecuteQuery, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, openconnection,lower_range,upper_range))
            thread[i].start()

        for i in range(5):
            thread[i].join()
            c.execute("INSERT INTO " + OutputTable + " SELECT * FROM OutputTable" + str(i) + "")
            openconnection.commit()




################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
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
    con.commit()
    con.close()


# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
