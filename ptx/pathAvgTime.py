#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import thread
from include.util import *
from math import ceil

def getPastHour():
    today = datetime.datetime.today()
    if int(today.strftime("%H")) < 6: 
        return None
    if int(today.strftime("%M")) < 15:
        return today + datetime.timedelta(hours=-2)
    return today + datetime.timedelta(hours=-1)
       
def avgPast(db, cursor, route, direction, deq, dst):
    table = getPastHour().strftime("%y_%m_%d_%H")
    sql = \
        """
        SELECT
            a.depseq, a.depstopid, s1.name, a.dstseq, a.dststopid, s2.name, AVG(a.avg_travel_time)
        FROM
            sb_avgtime.AvgTimeHourly_%s a , sb_ref.Stops s1, sb_ref.Stops s2
        WHERE
            a.depstopid = s1.stopid AND
            a.dststopid = s2.stopid AND
            a.routeid  = "%s" AND
            s1.routeid = "%s" AND
            s2.routeid = "%s" AND
            a.direction = %s
            
        GROUP BY
            a.depstopid, a.dststopid

        ORDER BY
            a.depseq
        """%( table, route, route, route, direction )
    cursor.execute( sql )
    avgTime = cursor.fetchall()
    total = 0.0
    start = deq
    for i in range( len(avgTime) ):
        if start != avgTime[i][1]:
            continue
        #print avgTime[i][6]
        total += avgTime[i][6]
        start = avgTime[i][4]
        if start == dst:
            break

    return total

def main():
    db, cursor = db_connect( db='sb_avgtime' )
    print avgPast(db, cursor, '0400081000','0','2205500060', '2425500562')/60
   



if __name__ == "__main__":
    main()
