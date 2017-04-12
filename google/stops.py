#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import re
from include.util import *
from math import ceil

time = str(int(time.time()))
url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=25.009599,121.457963&radius=300&types=bus_station&sensor=true&key=AIzaSyDveM91dVZcXP3wwYLcZN-VwLsGJVMMM8g"
def intoDB(data):
    if len(data) == 0:
        return
    db, cursor = db_connect()
    sql = "INSERT IGNORE INTO GoogleStops( place_id, name, lat, lng ) VALUES "
    db_commit(db, cursor, sql+ data);
    db.close()

def demo(data):
    for i, row in enumerate(data):
        if i < 5:
            for cel in row:
                if cel == "SubRoutes" :
                    for a in row[cel]:
                        print a
                #print cel.encode('utf-8') + ": "+ str(d[cel]).encode('utf-8')
            print " "

def loadProxies():
    with open('proxies') as f:
        content = f.read()
    plist =  content.split('\n')
    del plist[-1]
    return plist

def main():
    db, cursor = db_connect()

    key=  []
    key+= ["&key=AIzaSyAKF_sE3mbERVUVsHPrEXklLuZgtqSHbIY"]
    key+= ["&key=AIzaSyDveM91dVZcXP3wwYLcZN-VwLsGJVMMM8g"]
    key+= ["&key=AIzaSyCgc4CXyZGDXsDwq5cU5WzLBAE4yhPjw-g"]
    key+= ["&key=AIzaSyCN8xBHElGpbqD412SgVSMzG-dYY3ofKlc"]
    key+= ["&key=AIzaSyABbPaVLOfTNm9gygRJSlCaa97HiDuX4II"]
    key+= ["&key=AIzaSyDGvZu40mbcn-BgJSsM2SV2L3oziNyxnb8"]
    key+= ["&key=AIzaSyBXttcOIgETMSz3FDCW0HyTi11XuYyeYFU"]
    key+= ["&key=AIzaSyAlkULBFiJRNs4QTzM2IyFupxibBo9i4Lc"]
    key+= ["&key=AIzaSyCDBN85hrnkvKUq3PLcB8ZrVg0yuchcdiI"]
    key+= ["&key=AIzaSyDdK1BU7923fdJ24S5FzxF2bIemEjwf5DE"]
    key+= ["&key=AIzaSyBmiPqh4KrO0fQ8w0P4RINd1QzgHuzkKps"]
    key+= ["&key=AIzaSyCdhnBG1CAM39YcGV0LZlZv2UNLEs0VZ6E"]
    key+= ["&key=AIzaSyCo3uyiz7opi1iWsi7PwPOVjoC-rUKeocw"]
    key+= ["&key=AIzaSyATGBjcfrRXQtNEMMyt8Fw7tSGEY8PQwv0"]
    key+= ["&key=AIzaSyA7WavyokokzhJj4vD1-7DRkqjjOCdsVmk"]
    key+= ["&key=AIzaSyCQDgYLniH8zXKfbBwZRyYo_Eczx2xo0eY"]
    key+= ["&key=AIzaSyCoHL8zifqnOyminjWa9_6cruye-nQaTHE"]

    privous = 0
    keyExpect = []
    errorTimes = 0
    bar = progress(total=cursor.rowcount)
    fetch = cursor.fetchall()
    for i in range(len(fetch)):
        placeid, name = fetch[i]
        if i < 466:
            continue

        print str(name) + ": "+ str(placeid)
        while True:

            url = "https://maps.googleapis.com/maps/api/place/details/json?"
            url+= "placeid=%s"%(placeid)
            url+= "&language=zh-TW&region=TW"
            url+= key[ i%len(key) ]
            
            print url
            print
                        
            data = readURL(url)
            print data
            if errorTimes == len(key):
                quit()
            if str(data["status"]) != "OVER_QUERY_LIMIT":
                break
            errorTimes += 1
            print "OVER_QUERY_LIMIT"
        
        print data["result"]["url"]
        get = readContent(data["result"]["url"]+"&hl=zh_TW")
        print get
        results = ""
        for route in re.findall('"(\w+)",\w+,"#ffffff"', get.decode('UTF-8'), re.UNICODE):
            print route.encode("utf-8")
            results += ( ", " if len( results ) > 0 else " ")
            results += " ('%s', '%s') "%(placeid, route)

        intoDB(results)
        bar.getStep()
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)  
        print "Rows:::::" 
        cursor.execute("select count(*) from GoogleRoutes")
        print cursor.fetchone()
        
        bar.next()

    #demo()

if __name__ == "__main__":
    main()
