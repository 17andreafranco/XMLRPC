from pathlib import Path
import threading
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import pandas as pd
import threading,time

serverMaster = ServerProxy('http://localhost:8000',allow_none=True)

class MyFunctions:

    def __init__(self, df):
        self.df = df

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

class XMLRPCServerThread(threading.Thread):
     
    def __init__(self, port,csv, host = 'localhost'):
        self.host = host
        self.port = port
        self.csv = csv

        self.df = pd.read_csv(self.csv)

        threading.Thread.__init__(self)
        self.server = SimpleXMLRPCServer ((self.host, int(self.port)),logRequests=False)
        self.server.register_instance(MyFunctions(self.df))
   
    def run(self):
        print('Serving XML-RPC on localhost')
        self.server.serve_forever()

    def add(self):
        serverMaster.addURL("http://localhost:"+str(self.port))

    def remove(self):
        serverMaster.removeURL("http://localhost:"+str(self.port))

if __name__ == "__main__":

    try:
        w1 = XMLRPCServerThread(int(8002),Path("C:\\Users\\Andrea\\Documents\\Universitat\\3r\\Sistemes Distïbuits\\PRA 1\\CSV\\Catalunya.csv"))
        w1.add()
        w1.daemon=True
        w1.start()

        w2 = XMLRPCServerThread(int(8003),Path("C:\\Users\\Andrea\\Documents\\Universitat\\3r\\Sistemes Distïbuits\\PRA 1\\CSV\\Franca.csv"))
        w2.add()
        w2.daemon=True
        w2.start()

        while True: time.sleep(100)
    except (KeyboardInterrupt, SystemExit):
        w1.remove()
        w2.remove()
        print ('Received keyboard interrupt, quitting threads.\n')
    finally:
        print ('And its bye from me')

    
       
        

        
       