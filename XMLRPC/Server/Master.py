
import sys
from xmlrpc.server import SimpleXMLRPCServer

class MyFunctions:

    def __init__(self,workersURL):
        self.workersURL = workersURL

    def addURL(self,workerURL):
        self.workersURL.append(workerURL)
        return ("URL recived")

    def removeURL(self,workerURL):
        self.workersURL.remove(workerURL)
        return ("URL removed")
        
    def getURL(self):
        return (self.workersURL)

with SimpleXMLRPCServer(('localhost', 8000)) as server:
    server.register_instance(MyFunctions([]), allow_dotted_names=True)
    print('Serving XML-RPC on localhost port 8000')
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")
        sys.exit(0)
    