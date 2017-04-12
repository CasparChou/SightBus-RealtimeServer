#!/usr/bin/python
# -*- coding: utf-8 -*-.

# This is a program for fetching real bus stop position via google map api

import sys, time, datetime, requests
import re
from include.util import *
from math import ceil
from random import randint
time = str(int(time.time()))
url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=25.009599,121.457963&radius=300&types=bus_station&sensor=true&key=AIzaSyDveM91dVZcXP3wwYLcZN-VwLsGJVMMM8g"
def intoDB(data):
    if len(data) == 0:
        return
    db, cursor = db_connect()
    sql = "INSERT IGNORE INTO GoogleRoutes( place_id, name ) VALUES "
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

def main():
    db, cursor = db_connect()
    cursor.execute("SELECT place_id, name from GoogleStops \
            where place_id not in (select distinct place_id from GoogleRoutes) \
            ORDER BY place_id ASC")
    key=  []
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]
    key+= ["&key=API_KEY_HAS_BEEN_REMOVED"]

    privous = 0
    keyExpect = []
    errorTimes = 0
    errorcount = 0
    bar = progress(total=cursor.rowcount)
    fetch = cursor.fetchall()
    for i in range(len(fetch)):
        placeid, name = fetch[i]

        print str(name) + ": "+ str(placeid)
        while True:

            url = "https://maps.googleapis.com/maps/api/place/details/json?"
            url+= "placeid=%s"%(placeid)
            url+= "&language=zh-TW&region=TW"
            url+= key[ randint(0, len(key)-1) ]
            
            print url
            print
                        
            #data = readURL(url)
            response = requests.get(url, proxies={"http":"http://USER_NAME_HAS_BEEN_REMOVED:PASSWORD_HAS_BEEN_REMOVED@authproxy.fju.edu.tw:3128"})
            data = json.loads(response.text)
            if errorTimes == len(key):
                quit()
            if "status" not in data:
                quit()
            if data["status"] != "OVER_QUERY_LIMIT":
                break
            #errorTimes += 1
            #print "OVER_QUERY_LIMIT"
        
        print data["result"]["url"]
        #get = readContent(data["result"]["url"]+"&hl=zh_TW")
        response = requests.get(data["result"]["url"]+"&hl=zh_TW", proxies={"https":"https://USER_NAME_HAS_BEEN_REMOVED:PASSWORD_HAS_BEEN_REMOVED@authproxy.fju.edu.tw:3128"})
        get = response.text.encode("utf-8")
        print get
        if get.find("sorry") > 0:
            quit()
        results = ""
        for route in re.findall('"(\w+)",\w+,"#ffffff"', get.decode("utf-8"), re.UNICODE):
            print route.encode("utf-8")
            results += ( ", " if len( results ) > 0 else " ")
            results += " ('%s', '%s') "%(placeid, route)
        intoDB(results)
        bar.getStep()
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)
        print "Curr:::" + str(i)  
        bar.next()

    #demo()

if __name__ == "__main__":
    main()
