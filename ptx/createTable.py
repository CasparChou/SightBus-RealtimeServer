#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
from include.util import *
from math import ceil

#time = str(int(time.time()))

def wait_midnight():
    logi("Service idle")
    while True:
        year, mouth, day, hours, mins, sec = time_split()
        curr = ceil(time.time())
        if (int(hours) == 0 and int(mins) == 0 and int(sec) == 0) or (len(sys.argv) > 1 and sys.argv[1] == 'run'):
            try:
                sys.argv[1] = ""
                break
            except Exception, e:
                pass
        time.sleep(1)

def main():
    while True:
        wait_midnight()
        db, cursor = db_connect( )
        d = datetime.datetime.today()
        for i in range(24):
            if i in (2,3,4):
                continue
            k = d.replace(hour=i)
            kstr = k.strftime("%y_%m_%d_%H")
            cursor.execute("CREATE TABLE IF NOT EXISTS sb_history.History_%s LIKE sb_history.History;"%kstr) 
            cursor.execute("CREATE TABLE IF NOT EXISTS sb_arrival.Arrival_%s LIKE sb_arrival.Arrival;"%kstr)
            cursor.execute("CREATE TABLE IF NOT EXISTS sb_avgtime.AvgTimeHourly_%s LIKE sb_avgtime.AvgTimeHourly;"%kstr)



if __name__ == "__main__":
    main()
