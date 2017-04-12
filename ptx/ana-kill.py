import os
from datetime import datetime as dt
from time import sleep as sleep
from include.util import *

logi("Server supervisor activating")
run = False
while True:
    if len(sys.argv) > 1 :
        if sys.argv[1] == 'kill':
            run = True
    if dt.today().strftime("%M_%S") == "45_00" or run:
        logi("Clean-up old process")
        sleep(1)
        ps = os.popen("pgrep -f 'python ./analyze.py'").readlines()
        for i in ps:
            a = os.popen("kill %s"%(i.replace('\n','')))
        logi("All done!")
        run = False
