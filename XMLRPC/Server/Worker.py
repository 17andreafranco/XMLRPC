import datetime
from pathlib import Path
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import pandas as pd
import threading,time
import keyboard
import datetime


class MyFunctions:

    def __init__(self, df,leader = False,workersURL = [],workerTime=[],leaderURL=""):
        self.df = df

        self.workersURL = workersURL
        self.workerTime = workerTime

        self.leader = leader
        self.leaderURL = leaderURL

        self.t = threading.Thread(target=MyFunctions.howLong,args=(self,))
        self.t.daemon=True
        self.t.start()

    def readCSV(self):
        return (self.df.to_string())
    
    def applyCSV(self,changedColumn,column,request):
        self.df[changedColumn]=self.df[column].apply(eval(request)) 
        return (self.df.to_string())
  
    def columnsCSV(self):
        return (self.df.columns.to_list())

    def groupByCSV(self,nameColumn):
        grouped_df = self.df.groupby(nameColumn).agg(list)
        return (grouped_df.to_string())
    
    def headCSV(self,numRow):
        return (self.df.head(numRow).to_string())
    
    def isinCSV(self,column,num1,num2):
        new = self.df[column].isin([num1,num2])
        return (self.df[new].to_string())

    def itemsCSV(self):
        allItems = []
        for label,content in self.df.items():
            allItems.append('label:'+str(label)+ '\ncontent: \n'+str(content)+'\n')  
        return (allItems) 
    
    def minCSV(self,column):
        return (str(self.df[column].min()))

    def maxCSV(self,column):
        return (str(self.df[column].max()))
    

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

    def setLeader(self,leader,leaderURL):
        self.leader = leader
        self.leaderURL = leaderURL

    def getLeader(self):
        return self.leader

    def sendURL(self):
        serverAPI = ServerProxy('http://localhost:8001',allow_none=True)
        serverAPI.getURLLeader(self.leaderURL)

class XMLRPCServerThread(threading.Thread):
     
    def __init__(self, port,csv, masterURL='http://localhost:8000', host = 'localhost', workers = [], leader = False,first=True):
        self.host = host
        self.port = port
        self.csv = csv

        self.df = pd.read_csv(self.csv)

        threading.Thread.__init__(self)
        
        self.server = SimpleXMLRPCServer ((self.host, int(self.port)),logRequests=False)
        self.functions = MyFunctions(self.df)
        self.server.register_instance(self.functions)

        self.masterURL = masterURL
        self.serverMaster = ServerProxy(self.masterURL,allow_none=True)

        self.workers = workers
        self.leader = leader

        self.first = first

    def minWorker(self):
        ports = []
        for txt in self.workers:
            port =[int(s) for s in txt.split(":") if s.isdigit()]
            ports.append(str(port))
            
        return ports.index(min(ports))
    

    def stillAlive(self):
        date = datetime.datetime.now() 
        try:      
            self.workers = self.serverMaster.getURL()
            self.serverMaster.workerSayAlive(date.second,date.minute,"http://localhost:"+str(self.port))
            
        except (ConnectionRefusedError):
            self.masterURL = self.workers[XMLRPCServerThread.minWorker(self)]

            if (self.first):
                self.first = False
                for worker in self.workers:
                    if not(self.masterURL == "http://localhost:"+str(self.port)):
                        self.serverMaster = ServerProxy(self.masterURL,allow_none=True)
                        self.serverMaster.addURL(worker)
                        self.serverMaster.workerSayAlive(date.second,date.minute,"http://localhost:"+str(self.port))

                    else:
                        self.leader = True
                        self.functions.setLeader(self.leader,self.masterURL)
                        self.functions.addURL(self.masterURL)
                        self.functions.sendURL()      
            
            else:
               self.functions.sendURL()
                

    def run(self):
        print('Serving XML-RPC on localhost')
        self.server.serve_forever()
        
    def add(self):
        self.serverMaster.addURL("http://localhost:"+str(self.port))

    def remove(self):
        self.serverMaster.removeURL("http://localhost:"+str(self.port))

    def getWorkers(self):
        return self.serverMaster.getURL()
    

if __name__ == "__main__":

    try:

        w1 = XMLRPCServerThread(int(8002),Path("C:\\Users\\Andrea\\Desktop\\PRA_SD\\CSV\\Catalunya.csv"))
        w1.add()
        w1.daemon=True
        w1.start()   

        w2 = XMLRPCServerThread(int(8003),Path("C:\\Users\\Andrea\\Desktop\\PRA_SD\\CSV\\Franca.csv"))
        w2.add()
        w2.daemon=True
        w2.start()

        while True:   
            try: 
                w1.stillAlive() 
                w2.stillAlive()

                if keyboard.is_pressed('1'):
                    print('Worker 1 are stopped working')
                    w1.remove()
                    if (w1.getWorkers() == []):
                        break
                    
                elif keyboard.is_pressed('2'):
                    print('Worker 2 are stopped working')
                    w2.remove()
                    if (w2.getWorkers() == []):
                        break
            

            except(SystemExit):
                w1.remove()
                w2.remove()

            time.sleep(5.0)

    except (KeyboardInterrupt, SystemExit):
        w1.remove()
        w2.remove()
        print ('Received keyboard interrupt, quitting threads.\n')
    finally:
       print ('And its bye from me')
           
    
       
        

        
       