#!/usr/bin/python
# -*- coding: utf-8 -*-.
import sys, time, datetime
import Queue as queue, threading
from include.util import *
from math import ceil

class process:
    def __init__(self, route):
        self.route = route
   
    def do(self, thd, verbose = False):
        self.verbose = verbose
        self.db, self.cursor = db_connect(log=logt())
        sql = \
            """
            INSERT IGNORE INTO sb_ref.Links ( dep, dst )
            (
                SELECT
                    a.name, b.name
                FROM
                    sb_ref.Stops a, sb_ref.Stops b
                WHERE
                    a.routeid = '%s' AND b.routeid = '%s' AND
                    b.direction = a.direction AND
                    b.seq - a.seq = 1
                ORDER BY
                    a.direction, a.seq
            )
            """%(self.route, self.route)
        self.cursor.execute(sql)
        self.db.commit() 

        logi("%s path analyizing done !"%self.route, success=True)

     
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
    if True:
        thlen = 1
        startt = datetime.datetime.today()
        logi("Service running ...")
        
        que = queue.Queue()  
        db, cursor = db_connect()
        logi("Retreving routes...")
        routes = get_routes(cursor)
        for route in routes:
            que.put(process(route))
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
