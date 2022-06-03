
import datetime
import sys
import threading
import time

from xmlrpc.server import SimpleXMLRPCServer

class MyFunctions:

    def __init__(self,workersURL = [],workerTime=[]):
        self.workersURL = workersURL
        self.workerTime = workerTime

        self.t = threading.Thread(target=MyFunctions.howLong,args=(self,))
        self.t.daemon=True
        self.t.start()

    def addURL(self,workerURL):
        self.workersURL.append(workerURL)
        return ("URL recived")

    def removeURL(self,workerURL):
        self.workersURL.remove(workerURL)
        print("URL: "+workerURL+" removed")
        print(self.workersURL)
        return ("URL removed")
        
    def getURL(self):
        return (self.workersURL)

    def workerSayAlive(self,workerSeconds,workerMinute,workerURL):
        isIn = False
        time = {workerURL:[workerSeconds,workerMinute]}

        i = 0

        for items in self.workerTime:
            for worker in dict(items):
                if worker == workerURL:
                    self.workerTime[i] = time
                    isIn = True
            i += 1

        if (not isIn):
            self.workerTime.append(time)
        
        dateMaster = datetime.datetime.now()

        if ((dateMaster.second-workerSeconds > 20.0) or (dateMaster.minute - workerMinute > 1)):
            self.workersURL.remove(workerURL) 
               
        return("OK!")
        
    def howLong(self):
        while True:
            for items in self.workerTime:
                for worker in dict(items):
                    wTime=items[worker]
                    dateMaster = datetime.datetime.now() 
                    if(((dateMaster.second-wTime[0] > 20.0) or (dateMaster.minute - wTime[1] > 1))&(worker in self.workersURL)):
                        self.workersURL.remove(worker)
            time.sleep(5.0)
    
with SimpleXMLRPCServer(('localhost', 8000)) as server:
    f = MyFunctions()
    server.register_instance(f, allow_dotted_names=True)
    print('Serving XML-RPC on localhost port 8000')

    try:   
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)