#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import thread
from include.util import *
from math import ceil

def logger():
    city = sys.argv[1]
    return "[%s:%s] "%(city, getTime())

def intoDB(data):
    
    table = "\
        REPLACE INTO sb_realtime.BusPosition(\
            plateNum, routeId, direction, latitude, longitude, busStatus, recordtime\
        ) VALUES "

    db, cursor = db_connect(log=logger())
    sql = []
    print "%s%s rows will be inserted" % (logger(), str(len(data)))
    for i in range(len(data)):
        try:
            sql += ["( '%s', '%s', '%s', '%s', '%s', '%s', '%s' )"%(
                data[i]["PlateNumb"],
                data[i]["RouteID"],
                data[i]["Direction"],
                data[i]["BusPosition"]["PositionLat"],
                data[i]["BusPosition"]["PositionLon"],
                255 if "BusStatus" not in data[i] else data[i]["BusStatus"],
                data[i]["GPSTime"].replace("T", " ").replace("+08:00", "")
            )]
        except KeyError, e:
            logi("Key Error : %s"%e)
            print data[i]
   
    db_commit(db, cursor, table + ",".join(sql), log=logger());

    db.close()

def runData(url):
    data = readURL(url, log=logger())
    intoDB(data)

def getTime():
    return datetime.datetime.today().strftime("%H:%M:%S")

def main():
    if sys.argv[1] == "TPE":
        url = "http://ptx.transportdata.tw/MOTC/v2/Bus/RealTimeByFrequency/City/Taipei?%24format=JSON"
    else :
        url = "http://ptx.transportdata.tw/MOTC/v2/Bus/RealTimeByFrequency/City/NewTaipei?%24format=JSON"
    print "%sService startup"%logger()
    while True:
        wait_until(0, display=False)
        time.sleep(1)
        try:
            thread.start_new_thread(runData, (url,))
        except:
            print "%sError: unable to start thread"%logger()

if __name__ == "__main__":
    main()
