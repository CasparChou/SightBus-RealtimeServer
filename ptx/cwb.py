#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime, requests, xmltodict, json
from include.util import *

def download(url):
    logi("Downloading....")
    return requests.get(url).text


def analysis(resp):
    temp = []
    pops = []
    for i in resp["cwbopendata"]["dataset"]["locations"]["location"]:
        logi(i["locationName"].encode("utf-8"))
        city = i["locationName"]

        #Temperature
        for j in i["weatherElement"][1]["time"]:
            (tdate, ttime), value, measure = ( j["dataTime"].split("+")[0].split("T"), j["elementValue"]["value"], j["elementValue"]["measures"] )
            ttime = ttime.split(":")[0]
            temp += ["('%s', '%s', '%s', '%s')"%(city, tdate, ttime, value)]
            logi("%s %s: %s%s"%( tdate, ttime, value, measure ))

        #PoP6h
        for j in i["weatherElement"][8]["time"]:
            if isinstance(j["elementValue"]["value"],type(None)) :
                continue
            (pdate, ptime), pop = ((j["startTime"].split("+")[0]).split("T"), j["elementValue"]["value"])   
            ptime = ptime.split(":")[0]
            pops += ["('%s', '%s', '%s', '%s')"%(city, pdate, ptime, pop)]
            logi( "During:%s %s %s%%"%(pdate, ptime, pop))
    return temp, pops

def wait_hourly():
    logi("Service idle")
    while True:
        year, mouth, day, hours, mins, sec = time_split()
        curr = ceil(time.time())
        if (int(hours) in (0,3,6,9,12,15,18,21)  and int(mins) == 5 and int(sec) == 0):
            break
        time.sleep(1)

def main():
    key = AUTH_KEY_HAS_BEEN_REMOVED
    dataid = ["F-D0047-061", "F-D0047-069", "F-D0047-005"]
    url = "http://opendata.cwb.gov.tw/opendataapi?dataid=%s&authorizationkey=%s"
    while True:
        wait_hourly()
        data = [ 
                xmltodict.parse(download(url%(dataid[0], key))), 
                xmltodict.parse(download(url%(dataid[1], key))), 
                xmltodict.parse(download(url%(dataid[2], key))) 
            ]
        data = [ 
                json.loads(json.dumps(data[0])), 
                json.loads(json.dumps(data[1])),
                json.loads(json.dumps(data[2]))
            ]
        Tt, Tp = analysis(data[0])
        Nt, Np = analysis(data[1])
        Yt, Yp = analysis(data[2])

        d,c = db_connect(db="sb_realtime")
        db_commit(d,c,"""
            REPLACE INTO
                WeatherTemp
                (district, date, hour, degree)
            VALUES %s
            """%(",".join(Tt+Nt+Yt).encode("UTF8"))
        )
        db_commit(d,c,"""
            REPLACE INTO
                WeatherPoP
                (district, date, hour, degree)
            VALUES %s;
            """%(",".join(Tp+Np+Yp).encode("UTF8"))
        ) 
if __name__ == "__main__":
    main()
