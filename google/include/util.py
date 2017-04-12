#!/usr/bin/python
import urllib
import sys
import MySQLdb
import gzip, json
import time, datetime

from math import ceil
from BeautifulSoup import BeautifulSoup

def wait(stop=10):
    for i in range(stop):
        sys.stdout.write("----------- Wait for %s seconds ! %d s   -----------------\r" % (stop, stop-i) )
        sys.stdout.flush()
        time.sleep(1)

def wait_until(second):
    while True:
        year, mouth, day, hours, mins, sec = time_split()
        curr = ceil(time.time())
        sys.stdout.write("----------- Wait : %02d:%02d:%02d   -----------------\r" % (hours,mins,sec) )
        sys.stdout.flush()
        if sec in second:
            break
        time.sleep(1)


def time_split():
    strings = time.strftime("%Y,%m,%d,%H,%M,%S")
    t = strings.split(',')
    numbers = ( int(x) for x in t )
    return numbers

def downloadData(url, dst):
    sys.stdout.write("Downloading....")
    sys.stdout.flush()
    urllib.urlretrieve(url, dst)
    print "\b..success!"

def readURL(url):
    sys.stdout.write("Downloading....")
    sys.stdout.flush()
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    print "\b..success!"
    return data

def readContent(url):
    sys.stdout.write("Downloading....")
    sys.stdout.flush()
    response = urllib.urlopen(url)
    html = response.read()
    print "\b..success!"
    return html

def readf(dst):
    with open(dst, 'r') as reader:
        jdata = reader.read()
    data = json.loads(jdata)
    return data

def writef(dst, content):
    f = open(dst, 'w')
    f.truncate()
    f.write(content)
    f.close()

def db_connect():
    sys.stdout.write("MySQL Conntecting....")
    sys.stdout.flush()
    db = MySQLdb.connect("localhost","root", PASSWORD_HAS_BEEN_REMOVED, DATABASE_STR_HAS_BEEN_REMOVED)
    cursor = db.cursor()
    db.set_character_set('utf8')
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET time_zone = "+08:00";')
    cursor.execute('SET GLOBAL time_zone = "+8:00";')
    cursor.execute('SET character_set_connection=utf8;')
    print "..success!"
    return (db, cursor)

def db_commit( db, cursor, sql ):
    try:
        db.set_character_set('utf8')
        cursor.execute('SET NAMES utf8;')
        cursor.execute('SET time_zone = "+08:00";')
        cursor.execute('SET GLOBAL time_zone = "+8:00";')
        cursor.execute('SET character_set_connection=utf8;')
        cursor.execute(sql)
        db.commit()
        print " "
        print "Database Successful!"
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

def logi(msg,success=False):
    prefix = ""
    clear = ""
    if success == True:
        prefix = '\x1b[6;30;42m'
        clear = '\x1b[0m'
    sys.stdout.write(prefix+"[%s] %s\n"%(datetime.datetime.today().strftime("%H:%M:%S"), msg)+clear)
    sys.stdout.flush()

def logt():
    return "[%s] "%(datetime.datetime.today().strftime("%H:%M:%S"))

class progress:
    def __init__(self, total = 0):
        self.total = total
        self.steps = total / 100
        self.count = 0
    def getStep(self):
        print str(self.count/self.steps).rjust(3)+"%"


    def next(self):
        self.count += 1

    def log(self, msg = ""):
        if self.steps == 0:
            self.steps = 1
        if self.count%self.steps == 0:
            print ( \
    	        '\b'*(5)
                +str(self.count/self.steps).rjust(3)+"%"
            ),
            sys.stdout.flush()



