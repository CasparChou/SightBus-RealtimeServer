#!/usr/bin/python
import urllib
import sys
import pymysql
import gzip, json
import time, datetime

from math import ceil

def wait(stop=10):
    for i in range(stop):
        sys.stdout.write("----------- Wait for %s seconds ! %d s   -----------------\r" % (stop, stop-i) )
        sys.stdout.flush()
        time.sleep(1)

def wait_until(stop, display=True):
    while True:
        year, mouth, day, hours, mins, sec = time_split()
        curr = ceil(time.time())
        if display == True:
            sys.stdout.write("----------- Wait : %02d:%02d:%02d   -----------------\r" % (hours,mins,sec) )
            sys.stdout.flush()
        if (int(hours) >1 and int(hours)< 5):
            time.sleep(1)
            continue;
        elif int(hours) == 5 and int(mins) < 30:
            time.sleep(1)
            continue;
        elif sec in (0,10,20,30,40,50):
            break
        time.sleep(1)


def time_split():
    strings = time.strftime("%Y,%m,%d,%H,%M,%S")
    t = strings.split(',')
    numbers = ( int(x) for x in t )
    return numbers

def downloadData(url, dst, log=""):
    sys.stdout.write( "%sDownloading....\n"%log )
    sys.stdout.flush()
    urllib.urlretrieve(url, dst)

def readURL(url, log= ""):
    ticketurl = "http://ptx.transportdata.tw/MOTC/v2/Account/Login?UserData.account="+ACCOUNT_HAS_BEEN_REMOVED+"&UserData.password="+PASSWORD_HAS_BEEN_REMOVED+"&%24format=JSON"
    sys.stdout.write( "%sRetreving ticket....\n"%log )
    sys.stdout.flush()
    response = urllib.urlopen(ticketurl)
    data = json.loads(response.read())
    sys.stdout.write( "%stoken= %s \n"%(log,data["Ticket"]) )
    sys.stdout.flush()

    sys.stdout.write( "%sDownloading....\n"%log )
    sys.stdout.flush()
    response = urllib.urlopen(url+"&$ticket="+data["Ticket"])
    data = json.loads(response.read())
    return data

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

def db_connect( db=DEFAULT_DB_STR_HAS_BEEN_REMOVED, log="" ):
    sys.stdout.write(log + "MySQL Conntecting....\n")
    sys.stdout.flush()
    db = pymysql.connect("localhost","root", PASSWORD_HAS_BEEN_REMOVED, db, charset='utf8')
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET time_zone = "+08:00";')
    cursor.execute('SET GLOBAL time_zone = "+8:00";')
    return (db, cursor)

def db_commit( db, cursor, sql, log="" ):
    try:
        cursor.execute(sql)
        db.commit()
        sys.stdout.write("%sDatabase Successful!\n"%log )
        sys.stdout.flush()
    except pymysql.Error, e:
        sys.stdout.write("%sMysql Error %d: %s\n" % (log, e.args[0], e.args[1]))
        sys.stdout.flush()

def get_curr_table():
    year, mouth, day, hour, i,s = time_split()
    now = datetime.datetime.now().strftime("%y_%m_%d_%H")
    return {"arrived":"sb_arrival.Arrival_%s"%now, "history":"sb_history.History_%s"%now}
 

 
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
        self.count = 1

    def next(self):
        self.count += 1

    def log(self, prefix= ""):
        if self.steps == 0:
            self.steps = 1
        if self.count%self.steps == 0:
            print ('\b'*(5)
                +str(self.count/self.steps).rjust(3)+"%"),
            sys.stdout.flush()



