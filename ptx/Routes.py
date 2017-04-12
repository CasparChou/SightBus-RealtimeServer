#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
from include.util import *
from math import ceil


def intoDB(data):

    db, cursor = db_connect()
    bar = progress(total = len(data))
    print str( len(data) ) + " data[i]s will be inserted\n"
    sql = []
    for i in range(len(data)):
        sql +=[
            "('%s', '%s', '%s', '%s', '%s', '%s')"%(
            'TAO',
            data[i]["RouteID"],
            data[i]["RouteName"]["Zh_tw"],
            data[i]["DepartureStopNameZh"],
            data[i]["DestinationStopNameZh"],
            data[i]["TicketPriceDescriptionZh"]
        )]
        bar.log()
        bar.next()

    table = """
        INSERT IGNORE INTO 
            sb_ref.RoutesTMP(
                city, routeId, name, departure, destination, ticketPrice
            )
        VALUES """

    db_commit(db, cursor, table + ",".join(sql))

    db.close()

def main():
    #if sys.argv[1] == 'TPE':
    #    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/Route/City/Taipei?%24format=JSON"
    #else:
    #    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/Route/City/NewTaipei?%24format=JSON"
    url = "http://ptx.transportdata.tw/MOTC/v2/Bus/Route/City/NewTaipei?%24format=JSON"

    data = readURL(url)
    intoDB(data)


if __name__ == "__main__":
    main()
