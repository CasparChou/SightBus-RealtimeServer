#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import thread
from include.pyutil import *
from math import ceil

def logger():
    city = sys.argv[1]
    return "[%s:%s] "%(city,getTime())

def intoDB(sql, logtime):

    sqlH = "INSERT INTO %s(stopId, routeId, estimateTime, direction, recordtime ) VALUES "
    sqlA = "INSERT INTO %s(stopId, routeId, estimateTime, direction, recordtime ) VALUES "
    sqlE = "REPLACE INTO sb_realtime.Estimate(stopId, routeId, estimateTime, direction, recordtime ) VALUES "    
    db, cursor = db_connect( log=logger() )
    table = get_curr_table()
    db_commit(db, cursor, sqlE+sql, log=logger())
    if logtime[-2:] in ("00","30"):
        db_commit(db, cursor, sqlH%table["history"] +sql, log=logger())
        db_commit(db, cursor, sqlA%table["arrived"] +sql, log=logger())
    db.close()

def parserData(data, logtime):
    bar = progress(total = len(data))
    total = len(data)
    print "%s%s rows will be inserted" % (logger(), str(len(data)))
    sql = []
    sys.stdout.write("%sProcessing ..      "%logger() )
    sys.stdout.flush()
 
    for i in range(len(data)): 
        sql += ["('%s', '%s', '%s', '%s', '%s') "\
	    %(
            data[i]["StopID"],
            data[i]["RouteID"],
            -99 if "EstimateTime" not in data[i] else data[i]["EstimateTime"],
            data[i]["Direction"],
            data[i]["UpdateTime"].replace('T', ' ').replace('+08:00', '')
        )]
        bar.log()
        bar.next()
    print   
    intoDB(",".join(sql), logtime)


def runData(url, logtime):
    data = readURL(url, log=logger())
    parserData(data, logtime)

def getTime():
    return datetime.datetime.today().strftime("%H:%M:%S")

def main():
    city = sys.argv[1]
    if city == "TPE":
        url  = "http://ptx.transportdata.tw/MOTC/v2/Bus/EstimatedTimeOfArrival/City/Taipei?%24select=StopID%2CRouteID%2CEstimateTime%2CDirection%2CUpdateTime&%24format=JSON"
    else:
        url  = "http://ptx.transportdata.tw/MOTC/v2/Bus/EstimatedTimeOfArrival/City/NewTaipei?%24select=StopID%2CRouteID%2CEstimateTime%2CDirection%2CUpdateTime&%24format=JSON"
    
    logtime = time.strftime("%H:%M:%S")
    print "%sService startup"%(logger())
    while True:
        wait_until(0, display=False)
        logtime = time.strftime("%H:%M:%S")
        time.sleep(1)
        try:
            thread.start_new_thread( runData, (url, logtime ) )
        except:
            print "%sError: unable to start thread"%(logger())


if __name__ == "__main__":
    main()
