#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime, requests
import re
from include.util import *
from math import ceil
from random import randint

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
    cursor.execute("SELECT DISTINCT name, latitude, longitude from Stops WHERE city = 'TAO'")
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
    table = "INSERT IGNORE INTO GoogleStops ( place_id, name, latitude, longitude ) VALUES "
    privous = 0
    keyi = 0
    errorTimes = 0
    errorcount = 0
    fetch = cursor.fetchall()
    for i in range(836, len(fetch)):
        name, lat, lng = fetch[i]

        
        logi("Scan at %s : %s, %s ::: use key id %d, place id %d compelete %d%%"%(name, lat, lng, keyi, i, i/len(fetch)))
         
        while True:
            url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
            url+= "location=%s,%s&radius=300&types=bus_station&sensor=true"%(lat, lng)
            url+= "&language=zh-TW&region=TW"
            url+= key[ keyi ]
            response = requests.get(url, proxies={"http":"http://USER_NAME_HAS_BEEN_REMOVED:PASSWORD_HAS_BEEN_REMOVED@authproxy.fju.edu.tw:3128"})
            data = json.loads(response.text)
            if data["status"] == "OVER_QUERY_LIMIT":
                keyi += 1
                if keyi == len(key):
                    logi("Overlimit at id %s"%i)
                    quit()
                logi("Change key to id %d"%keyi)
                continue
            else:
                break
            
        if len(data["results"])  > 0:
            d, c = db_connect()
            results = []
            for rows in data["results"]:
                place_id = rows["place_id"]
                name = rows["name"]
                latitude = rows["geometry"]["location"]["lat"]
                longitude = rows["geometry"]["location"]["lng"]
                logi("Get bus stop %s %s: %s, %s "%(place_id.encode('utf-8'), name.encode('utf-8'), latitude, longitude))
                results += ["('%s', '%s', '%s', '%s')"%(place_id, name, latitude, longitude)]
            #print ",".join(results).encode("utf-8")
            db_commit( d, c, table + ",".join(results).encode("utf-8") )
            d.close()

if __name__ == "__main__":
    main()
