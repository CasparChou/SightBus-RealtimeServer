#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import thread
from include.util import *
from math import ceil

def queryDir(db, cursor, dep, dst):
    sql = \
    """
        SELECT
            DISTINCT stopid, name, routeid
        FROM 
            Stops 
        WHERE 
            name = '%s'
        GROUP BY
    """
    cursor.execute( sql%dep )
    stopa = cursor.fetchall()
    stopalen = cursor.rowcount
    cursor.execute( sql%dst )
    stopb = cursor.fetchall()
    stopblen = cursor.rowcount

    for j in range( stopalen ):
           #print pathGet(db, cursor, stopa[i][0], stopb[j][0])
        print "%s %s %s"%(stopa[j][0], stopa[j][1], stopa[j][2])
    for j in range( stopblen ):
        print "%s %s %s"%(stopb[j][0], stopb[j][1], stopb[j][2])
        
def pathGet(db, cursor, dep, dst):
    getpath =\
    """
        SELECT
            *
        FROM
            Path
        WHERE 
            dep = '%s' AND
            dst = '%s' 

    """%(dep, dst)

    if cursor.execute(getpath) > 0L:
        paths = cursor.fetchall()
        for j in range( len(paths) ):
            print "%s %s %s %s"%(paths[j][0], paths[j][1], paths[j][2], paths[j][3])

        return paths
    else:
        pathfinding = """
            INSERT INTO Path( dep, dst, routeid, direction )
            (
                SELECT
                    a.name, b.name, a.routeid, a.direction
                FROM
                    Stops a, Stops b
                WHERE
                    a.name = '%s' AND
                    b.name = '%s' AND
                    a.seq < b.seq AND
                    a.direction = b.direction AND
                    a.routeid = b.routeid
            )
        """%(dep, dst)
        if db_commit(db, cursor, pathfinding) > 0:            
            return pathGet(db, cursor, dep, dst)
    
def main():
    db, cursor = db_connect( db='sb_ref' )
    #queryDir(db, cursor, "北門街","捷運輔大站")
    #quit()

    #print pathGet(db, cursor, '2205200200','2425500562')
    #print pathGet(db, cursor, '2205000220','2425500562')
    #print pathGet(db, cursor, '2205000560','2425500562')
    paths = pathGet(db, cursor, '捷運府中站', '捷運輔大站')
   



if __name__ == "__main__":
    main()
