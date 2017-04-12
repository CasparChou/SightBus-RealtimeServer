#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
from include.util import *
from math import ceil

def intoDB(data):

    db, cursor = db_connect()
    bar = progress(total = len(data))
    print str(len(data)) + " rows will be inserted\n"
    sql = []
    for i in range(len(data)):
        for j in range(len(data[i]["Stops"])):
            sql += [
                "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(
                'TAO',
                data[i]["Stops"][j]["StopID"],
                data[i]["RouteID"],
                data[i]["Stops"][j]["StopName"]["Zh_tw"],
                data[i]["Direction"],
                data[i]["Stops"][j]["StopSequence"],
                data[i]["Stops"][j]["StopBoarding"],
                data[i]["Stops"][j]["StopPosition"]["PositionLat"],
                data[i]["Stops"][j]["StopPosition"]["PositionLon"]
             )]
        bar.log()
        bar.next()

    table = """
        INSERT IGNORE INTO
            sb_ref.StopsTMP (
                city, stopId, routeid, name, direction, seq, boarding, latitude, longitude
            )
        VALUES """

    db_commit(db, cursor, table + ",".join(sql));
    db.close()

def main():
    #if sys.argv[1] == 'TPE':
    #    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/StopOfRoute/City/Taipei?%24format=JSON"
    #else:
    #    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/StopOfRoute/City/NewTaipei?%24format=JSON"
    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/StopOfRoute/City/Taoyuan?%24format=JSON"
    data = readURL(url)
    intoDB(data)


if __name__ == "__main__":
    main()
