from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import numpy as np
import json

data = {}   # To store the flow information
host = ('localhost', 17777)     #HTTP server port

class NpEncoder(json.JSONEncoder):
    '''
    To encoder the data type in numpy 
    '''
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

class HTTPinterface(threading.Thread):
    '''
    This is a thread running the HTTPserver for flows infomation query
    '''
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        main()

class Resquest(BaseHTTPRequestHandler):
    '''
    This is the HTTPserver request
    '''
    def do_GET(self):
        if self.path == '/favicon.ico':
            return
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, cls=NpEncoder).encode())

def main():
    '''
    For HTTPserver start
    '''
    server = HTTPServer(host, Resquest)
    print("Starting server, listen at: %s:%s" % host)
    server.serve_forever()