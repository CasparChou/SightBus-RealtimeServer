#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import Queue as queue, threading
from include.util import *
from math import ceil

def match_all_stops(c, route):
    sql = """
        SELECT
            a.stopid, b.stopid /*a.name, b.name, a.seq, b.seq*/
        FROM 
            sb_ref.Stops a, sb_ref.Stops b 
        WHERE
            a.seq < b.seq AND
            a.direction = b.direction AND
            a.routeid = "%s" AND
            a.routeid = b.routeid
        ORDER BY 
            a.direction, a.seq, b.seq;
        """%route
    c.execute(sql)
    rs = c.fetchall()
    for i in rs:
        data += [i]
    return data
 
def wait_hourly():
    logi("Service idle")
    while True:
        year, mouth, day, hours, mins, sec = time_split()
        curr = ceil(time.time())
        if (int(hours) not in (3,4,5)  and int(mins) == 0 and int(sec) == 0):
            break
        time.sleep(1)
class process:
    def __init__(self, datehour, route):
        self.datehour = datehour
        self.route = route
   
    def do(self, thd, verbose = False):
        self.verbose = verbose
        self.db, self.cursor = db_connect(log=logt())
        maxdep, maxdst = self.fetchDepDst()

        #timetable = self.retrieve_time()
        #results = self.analysis(timetable, maxdep, maxdst, False) 
        #self.new_update(results)

        timetable = self.new_retrieve_time()
        results = self.new_analysis(timetable, maxdep, maxdst, False)
        self.update(results)

        logi("%s AvgTime Already done !"%self.route, success=True)

    def retrieve_time(self):
        logi("Retreving %s timetable..."%self.route)
        sql = """
            SELECT s.seq, s.name, t.stopid, t.direction, t.time, "0", t.recordtime FROM (
                SELECT
                     *, FROM_UNIXTIME(  120*CEIL((UNIX_TIMESTAMP(recordtime) + estimatetime)/120)) as time
                FROM
                    sb_arrival.Arrival_%s
                WHERE
                    routeid = "%s" AND
                    estimateTime BETWEEN 0 AND 120
                GROUP BY
                    concat(stopid, time)
            ) AS t, sb_ref.Stops s
            WHERE
                s.routeid = "%s" AND
                s.stopid = t.stopid
    
            ORDER BY
                t.direction, t.time, s.seq     
        """%(self.datehour, self.route, self.route)
        self.cursor.execute(sql)    
        data = []
        rs = self.cursor.fetchall()
        for i in rs:
            data += [list(i)]
        return data
    def new_retrieve_time(self):
        logi("Retreving %s timetable..."%self.route)
        sql = """
            SELECT s.seq, s.name, t.stopid, t.direction, t.time, "0", t.recordtime FROM (
                SELECT
                     *, FROM_UNIXTIME(  120*CEIL((UNIX_TIMESTAMP(recordtime) + estimatetime)/120)) as time
                FROM
                    sb_arrival.Arrival_%s
                WHERE
                    routeid = "%s" AND
                    estimateTime BETWEEN 0 AND 120
                GROUP BY
                    concat(stopid, time)
            ) AS t, sb_ref.Stops s
            WHERE
                s.routeid = "%s" AND
                s.stopid = t.stopid

            ORDER BY
                t.direction,s.seq,  t.time
        """%(self.datehour, self.route, self.route)
        self.cursor.execute(sql)
        data = []
        rs = self.cursor.fetchall()
        for i in rs:
            data += [list(i)]
        return data

    def new_analysis(self, data, maxdep, maxdst, verbose=False):
        logi(" %s Analyzing... "%self.route)   
        results = []
        pre = -1
        stop = 0
        while pre != 0:
            if verbose:
                print "------------------------------------------"
                print "PRE: %s"%pre
            stop += 1
            pre = 0
            cseq = -1
            cdir = -1
            tmp = []
            for i in range(len(data)):
                if data[i][5] == 1:
                    continue
                pre+= 1
                if cseq == -1:
                    cseq, cdir, ctime = data[i][0], data[i][3], int(time.mktime(data[i][4].timetuple()))
                    if verbose:
                       print "\033[91m      |- SEARCH (%d) %s %s %s \033[0m"%(i, cseq, cdir, ctime)
                elif (data[i][0] != cseq or data[i][3] != cdir):
                    if verbose:
                        d = data[i]
                    #    print "\033[90m      |- SKIP (%d) %s %s %s %s %s \033[0m"%(i, d[0], d[1], d[2], d[3], d[4])
                    continue

#                for remove in range(i,len(data)):
#                    if data[remove][5] == 1:
#                        continue
#                    if data[remove][0] != cseq or data[remove][3] != cdir or (ctime - int(time.mktime(data[remove][4].timetuple())) > 180):
#                        break
#                    data[remove][5] = 1
#                    if verbose:
#                        d = data[remove]
#                        print "\033[91m      |- DELETE (%d) %s %s %s %s %s \033[0m"%(remove, d[0], d[1], d[2], d[3], d[4])

                update = i
                for seek in range(i, len(data)):
                    ctime = int(time.mktime(data[update][4].timetuple()))
                    if data[seek][5] == 1:
                        continue
                    if data[seek][0] != cseq or data[seek][3] != cdir or (int(time.mktime(data[seek][4].timetuple())) - ctime) > 180:
                        if verbose:
                            print "\033[94m      |- TRY Finished (dst:%d) \033[0m"%(seek)
                        break
                    data[seek][5] = 1
                    update = seek
                    if verbose:
                        d = data[seek]
                        print "\033[92m      |- MATCH (%d) %s %s %s %s %s \033[0m"%(seek, d[0], d[1], d[2], d[3], d[4])

                if verbose:
                    logi("CURRI: %d | UPDATE:%d | CURRSEQ:%d "%(i, update, cseq))
                

                d = data[update]
                if len(tmp) > 0:
                    delta = (d[6] - tmp[(len(tmp)-1)][6])
                    delta = delta if delta >= datetime.timedelta(minutes=0) else datetime.timedelta(minutes=0)
                    if verbose:
                        print "   | %s"%(delta)
                    results += ["('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(self.route, tmp[(len(tmp)-1)][0], tmp[(len(tmp)-1)][2], d[0], d[2], d[3], delta.total_seconds(), d[6])]
                if verbose:
                    print " * |- (%d) %s %s %s %s %s"%(update, d[0], d[1], d[2], d[3], d[4])
    #                print " * |- %s %s %s %s"%(d[0], d[1], d[3], d[4])
                tmp += [data[update]]
                cseq = data[update][0] + 1
                if verbose:
                    logi("CSEQ 1 : %d"%cseq)
                if cdir == 0 and cseq == maxdst+1:
                    break
                if cdir == 1 and cseq == maxdep+1:
                    break
        return results
 
    
    def analysis(self, data, maxdep, maxdst, verbose=False):

        logi("Analizing...")
        results = []
        pre = -1
        while pre != 0:
            if verbose:
                print "------------------------------------------"
                print "PRE: %s"%pre
            pre = 0
            cseq = -1
            cdir = -1
            tmp = []
            for i in range(len(data)):
                if data[i][5] == 1:
                    continue
                else:
                    pre+= 1
                if cseq == -1:
                    cseq = data[i][0]
                    cdir = data[i][3]
    
    
                for remove in range(0,i):
                    if data[remove][5] == 1:
                        continue
                    if int(time.mktime(data[i][4].timetuple())) - int(time.mktime(data[remove][4].timetuple())) > 180:
                        continue
                    if data[remove][0] == cseq and data[remove][3] == cdir:
                        data[remove][5] = 1
                        if verbose:
                            d = data[remove]
                            print "\033[91m      |- DELETE (%d) %s %s %s %s %s \033[0m"%(remove, d[0], d[1], d[2], d[3], d[4])
                if verbose:    
                    logi("CSEQ 0 : %d"%cseq)
                update = i
                for seek in range(i, len(data)):
                    if data[seek][5] == 1:
                        continue
                    if int(time.mktime(data[seek][4].timetuple())) - int(time.mktime(data[update][4].timetuple())) > 180:
                        if verbose:
                            print "\033[94m      |- TRY Finished (dst:%d) \033[0m"%(seek)
                        break
                    d = data[seek]
                    if data[seek][0] == cseq and data[seek][3] == cdir:
                        data[seek][5] = 1
                        update = seek
                        if verbose:
                            print "\033[92m      |- MATCH (%d) %s %s %s %s %s \033[0m"%(seek, d[0], d[1], d[2], d[3], d[4])
                    else:
                        if verbose:
                            print "\033[94m      |- TRY (%d) %s %s %s %s %s \033[0m"%(seek, d[0], d[1], d[2], d[3], d[4])
                if verbose:
                    logi("CURRI: %d | UPDATE:%d | CURRSEQ:%d "%(i, update, cseq))
                
                d = data[update]
                if verbose:
                    if (data[i][0] != cseq or data[i][3] != cdir):
                        d = data[i]
                        print "\033[90m      |- SKIP (%d) %s %s %s %s %s \033[0m"%(i, d[0], d[1], d[2], d[3], d[4])
                if (data[update][0] == cseq and data[update][3] == cdir):
                    d = data[update]
                    if len(tmp) > 0:
                        delta = (d[6] - tmp[(len(tmp)-1)][6])
                        delta = delta if delta >= datetime.timedelta(minutes=0) else datetime.timedelta(minutes=0)
                        if verbose:
                            print "   | %s"%(delta)
                        results += ["('%s', '%s', '%s', '%s', '%s', '%s')"%(self.route, tmp[(len(tmp)-1)][2], d[2], d[3], delta.total_seconds(), d[6])]
                    if verbose:
                        print " * |- (%d) %s %s %s %s %s"%(update, d[0], d[1], d[2], d[3], d[4])
    #                print " * |- %s %s %s %s"%(d[0], d[1], d[3], d[4])
                    tmp += [data[update]]
                    cseq = data[update][0] + 1
                if verbose:
                    logi("CSEQ 1 : %d"%cseq)
                if cdir == 0 and cseq == maxdst+1:
                    break
                if cdir == 1 and cseq == maxdep+1:
                    break
    
        return results
    
    def fetchDepDst(self):
        logi("Retreving %s DepDst..."%self.route)
        self.cursor.execute(
        """
            SELECT
                count(IF(direction= 1,1,NULL)),
                count(IF(direction= 0,1,NULL))
            FROM 
                sb_ref.Stops
            WHERE routeid = "%s"
        """%self.route)
        return self.cursor.fetchone()
    def new_update(self, results):
        thefile = open('%s.log'%datetime.datetime.today().strftime("%H-%M-%S") , 'w')
        for item in results:
              thefile.write("%s\n" % item)
        thefile.close()

    def update(self, results):
        logi("Updating %s"%self.route)
        db_commit(
            self.db, self.cursor,
            """
                INSERT INTO
                    sb_avgtime.AvgTimeHourly_%s
                VALUES
                    %s
            """%( self.datehour, ",".join(results) ))
     
def get_routes(cursor):
    cursor.execute("SELECT routeid FROM sb_ref.Routes")
    data = []
    rs = cursor.fetchall()
    for i in rs:
        data += [i[0]]
    return data
 

   
def doJob(*args):  
    queue = args[0]
    thd = args[1]
    return
    while queue.qsize() > 0:
        for i in range(5):
            if not thd[i].is_alive():
                job = queue.get()
                job.do()
def ru(a):
    print a+ " is running"


def main():
    while True:
        wait_hourly()
        thlen = 10
        datehour = datetime.datetime.today() + datetime.timedelta(hours=-1)
        datehour = datehour.strftime("%y_%m_%d_%H")
        startt = datetime.datetime.today()
        logi("Service running ... target = %s"%datehour)
        
        que = queue.Queue()  
        db, cursor = db_connect()
        logi("Retreving routes...")
        routes = get_routes(cursor)
        for route in routes:
            que.put(process(datehour, route))
        thd = []
        for i in range(thlen):
            job = que.get()
            thd += [
                    threading.Thread(
                        target=job.do,
                        name='Thd%d'%i, 
                        args=('Thd%d'%i,))
                    ]
            thd[i].start()
        while que.qsize() > 0:
            for i in range(thlen):
                if not thd[i].is_alive():
                    job = que.get()
                    thd[i] = threading.Thread(
                            target=job.do, name='Thd%d'%i, args=("Thd%d"%i,))
                    thd[i].start()
        for i in range(thlen):
            thd[i].join()
        logi("Analization success!")
        logi("Start: %s"%startt.strftime("%H:%M:%S"))
        logi("Finished: %s"%datetime.datetime.today().strftime("%H:%M:%S"))

#        print cursor._executed

if __name__ == "__main__":
    main()
