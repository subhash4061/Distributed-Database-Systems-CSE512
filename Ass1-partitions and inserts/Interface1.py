import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    c = openconnection.cursor()
    c.execute("DROP TABLE IF EXISTS " + ratingstablename + ";")
    c.execute("CREATE TABLE " + ratingstablename + "(userid int,empty1 varchar,movieid int,empty2 varchar,rating real,empty3 varchar,time_stamp int)")
    openconnection.commit()
    input = open(ratingsfilepath)
    c.copy_from(input, ratingstablename, sep=':',
                columns=('userid', 'empty1', 'movieid', 'empty2', 'rating', 'empty3', 'time_stamp'))
    c.execute("ALTER TABLE Ratings DROP COLUMN empty1, DROP COLUMN empty2, DROP COLUMN empty3, DROP COLUMN time_stamp;")
    openconnection.commit()
    c.execute("DROP TABLE IF EXISTS metatable;")
    c.execute("CREATE TABLE metatable(type varchar,value int)")
    openconnection.commit()
    c.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    rangeno = int(numberofpartitions)
    c.execute("INSERT INTO metatable(type,value) VALUES ('range'," + str(rangeno) + ")")
    openconnection.commit()
    range_cond = float(5.0 / numberofpartitions)
    for i in range(0, rangeno):
        if i == 0:
            j = float(i)
            c.execute("DROP TABLE IF EXISTS range_ratings_part" + str(i) + ";")
            c.execute("CREATE TABLE range_ratings_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE rating>=" + str(j * range_cond) + " AND rating<=" + str((j + 1) * range_cond) + ";")
            openconnection.commit()
        else:
            j = float(i)
            c.execute("DROP TABLE IF EXISTS range_ratings_part" + str(i) + ";")
            c.execute("CREATE TABLE range_ratings_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE rating> " + str(j * range_cond) + " AND rating<=" + str((j + 1) * range_cond) + ";")
            openconnection.commit()
    c.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    part_list = list(range(numberofpartitions))
    round_part = int(numberofpartitions)
    c.execute("INSERT INTO metatable(type,value) VALUES ('round',"+str(round_part)+")")
    openconnection.commit()
    last_insert = 0


    for i in part_list:
        c.execute("DROP TABLE IF EXISTS round_robin_ratings_part" + str(i) + ";")
        if i == int(numberofpartitions) - 1:
            c.execute("CREATE TABLE round_robin_ratings_part" + str(i) + " AS SELECT * FROM (SELECT row_number() over() as row_id,* FROM " + ratingstablename + ") as temp WHERE row_id % " + str(round_part) + " = " + str(i+1-int(numberofpartitions)))
            openconnection.commit()
        else:
            c.execute("CREATE TABLE round_robin_ratings_part" + str(i) + " AS SELECT * FROM (SELECT row_number() over() as row_id,* FROM " + ratingstablename + ") as temp WHERE row_id % " + str(round_part) + " = "+str(i+1))
            openconnection.commit()
        c.execute("SELECT count(*) FROM round_robin_ratings_part" + str(i) + ";")
        p_row=c.fetchone()[0]
        last_insert=p_row%round_part


    c.execute("INSERT INTO metatable(type,value) VALUES ('lastinsert',"+str(last_insert)+")")
    openconnection.commit()
    c.close()




def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    c.execute("SELECT value FROM metatable WHERE type='round'")
    round_part=c.fetchone()[0]
    c.execute('SELECT COUNT(*) from {0}'.format(ratingstablename))
    count = int(c.fetchone()[0])
    new_insert = (count) % (round_part)
    c.execute("INSERT INTO round_robin_ratings_part" + str(new_insert) + " (userid,movieid,rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    openconnection.commit()
    c.close()




def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    c=openconnection.cursor()
    c.execute("SELECT value FROM metatable WHERE type='range'")
    numberofpartitions=c.fetchone()[0]
    range= float(5/numberofpartitions)
    lower_bound=0
    upper_bound=range
    part_no=0

    while lower_bound<5:
        if lower_bound==0:
            if rating >= lower_bound and rating<=upper_bound:
                break
            part_no = part_no + 1
            lower_bound = lower_bound + range
            upper_bound = upper_bound + range
        else:
            if rating > lower_bound and rating <= upper_bound:
                break
            part_no = part_no + 1
            lower_bound = lower_bound + range
            upper_bound = upper_bound + range
    c.execute("INSERT INTO range_ratings_part"+str(part_no)+" (userid,movieid,rating) VALUES ("+ str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    openconnection.commit()
    c.execute("INSERT INTO ratings (userid,movieid,rating) VALUES ("+ str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    openconnection.commit()
    c.close()








def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    c=openconnection.cursor()
    final=[]
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%ratings_part'")
    for tab in c:
        c.execute("select * from "+ str(tab[0])+" where ratings<= "+ str(ratingMaxValue)+ " and rating >="+str(ratingMinValue))
        all1=c.fetchall()
        if len(all1)!=0:
            for l in all1:
                y=(tab[0],l[1],l[2],l[3])
                final.append(y)
    file=open(outputPath,'w')

    for i in final:
        file.write(','.join(str(string) for string in i) + '\n')

    c.close()
    file.close()




def pointQuery(ratingValue, openconnection, outputPath):
    c = openconnection.cursor()
    final = []
    c.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%ratings_part'")
    for tab in c:
        c.execute("select * from " + str(tab[0]) + " where ratings = " + str(ratingValue))
        all1 = c.fetchall()
        if len(all1) != 0:
            for l in all1:
                y = (tab[0], l[1], l[2], l[3])
                final.append(y)
    file = open(outputPath, 'w')

    for i in final:
        file.write(','.join(str(string) for string in i) + '\n')

    c.close()
    file.close()




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
