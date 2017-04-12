#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import thread
import types
from include.util import *
from math import ceil

depdst = """
    (
        SELECT
            (latitude) AS dep_lat,  (longitude) AS dep_lng
        FROM
            sb_ref.Stops
        WHERE
            name = '{dep}'
        LIMIT 1
    ) AS deploc,
    (
        SELECT
            (latitude) AS dst_lat,  (longitude) AS dst_lng
        FROM
            sb_ref.Stops
        WHERE
            name = '{dst}'
        LIMIT 1
    ) AS dstloc"""

sql = """ 
    SELECT 
        link.*, CONCAT((latitude), ',',  (longitude)) AS nextloc, 
        SQRT(
            POW(69.1 * (latitude - dst_lat),  2) +
            POW(69.1 * (dst_lng - longitude) * COS(latitude / 57.3), 2)
        ) AS distance
    FROM
    (
        SELECT
            linkid, dst, dep_lat, dep_lng, dst_lat, dst_lng
        FROM
            {depdst},
            sb_ref.Links
        WHERE
            dep = '{dep}'
        ORDER BY
            linkid
    ) AS link, sb_ref.Stops s
    WHERE
        link.dst = s.name
    GROUP BY
        link.dst
    ORDER BY
        distance
        """
routeSql = """\
    SELECT
        DINTINCT r.name
    FROM
        sb_ref.Routes r, sb_ref.Stops s
    WHERE
        s.routeid = r.routeid AND
        s.name = '{dep}'
    """

def paser(d):
    return \
            {
                "link":d[0],
                "name":"%s"%d[1],
                "dep":{
                    "lat":d[2],
                    "lng":d[3],
                },
                "dst":{
                    "lat":d[4],
                    "lng":d[5],
                },
                "next":{
                    "lat":d[6].split(",")[0],
                    "lng":d[6].split(",")[1],
                },
                "far":d[7]
            }


class Node:
    def __init__(self, **argv):
        self.__dict__.update(**argv)
        for k,v in self.__dict__.items():
            if type(v) == types.DictType:
                setattr(self, k, Node(**v))
    def __str__(self):
        return "{link:%s, name:%s, next:%s, far:%s, dep:%s, dst:%s}"%(
                self.link, self.name, 
                self.next.lat+","+self.next.lng,
                self.far,
                self.dep.lat+","+self.dep.lng,
                self.dst.lat+","+self.dst.lng
                )

def pathGet(db, cursor, dep, dst, route=None, seq=None):
    global sql
    global depdst
    run = sql.replace("{depdst}",depdst).format(**locals())
    cursor.execute( run )
    links = []
    logi("results : %d rows"%cursor.rowcount)
    for d in range(cursor.rowcount):
        links += [Node(**paser(cursor.fetchone()))]
        print "\33[90m%s\33[0m"%links[-1]

    return links

   
def utuple(t):
    return repr(t).decode('raw_unicode_escape')

def main():
    db, cursor = db_connect( db='sb_ref' )
    dst = '捷運江子翠站'
    past = []
    
    s = 50
    get = pathGet(db, cursor, '捷運迴龍站', dst)
    while s > 0:
        target = get[0]
        s -= 1
        for i in range( len(get) ):
            if get[i].name not in past:
                print "\33[32m%s\33[0m"%get[i]
                past += [get[i].name]
                target = get[i]
                break
            else:
                print past
                print "\33[31m%s\33[0m"%get[i]
        if target.name == dst:
            print "\33[32m Destiantion Arrival (%d steps) \33[0m"%(50-s)
            break
        get = pathGet(db, cursor, target.name, dst)
        
        time.sleep(1.4)



if __name__ == "__main__":
    main()
