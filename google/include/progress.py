import sys


class ProgressBar:

    def __init__(self, total = 0):
        self.total = total
	self.steps = total / 100
        self.count = 0

    def next(self):
        self.count += 1

    def log(self, msg = ""):
	
        if self.count%self.steps == 0:
            print ( \
    	        '\b'*(5)
                +str(self.count/self.steps).rjust(3)+"%"
            ),
            sys.stdout.flush()



