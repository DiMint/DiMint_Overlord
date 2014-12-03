import threading, zmq, json

active_request = {}
active_result = {}

class ZmqThreadForClient(threading.Thread):
    def __init__(self, config, nodes, nodes_lock, event):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_client']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % self.port)
        self.event = event

    def run(self):
        while True:
            message = self.socket.recv()
            result = self.__process_request(message)
            response = json.dumps(result).encode('utf-8')
            print ('Response {0}'.format(response))
            self.socket.send(response)
            #self.nodes_lock.acquire()
            # use nodes
            #print(self.nodes)
            #self.nodes_lock.release()

    def __process_request(self, message):
        global active_request, active_result
        print('Request {0}'.format(message))
        response = {}
        request = json.loads(message.decode('utf-8'))
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            if cmd == 'get_overloads':
                response['overloads'] = self.config['overloads']
            elif cmd == 'get' or cmd == 'set':
                active_request = message
                self.event.clear()
                self.event.wait()
                response['value'] = active_result['value']
            else:
                response['error'] = 'DIMINT_NOT_FOUND'
        except:
            response['error'] = 'DIMINT_PARSE_ERROR'
        return response

class ZmqThreadForNode(threading.Thread):
    def __init__(self, config, nodes, nodes_lock, event):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_node']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://{0}:{1}".format('127.0.0.1', self.port))
        self.event = event

    def run(self):
        global active_request, active_result
        while True:
            if not self.event.is_set():
                message = active_request
                self.socket.send(message)
                result = self.socket.recv()
                active_result = json.loads(result.decode('utf-8'))
                self.event.set()

class Overlord:
    def __init__(self, config_path=""):
        self.nodes = []
        self.nodes_lock = threading.Lock()
        self.event = threading.Event()
        if (config_path == ""):
            config_path = './dimint_server.config'
        with open(config_path, 'r') as config_data:
            self.config = json.loads(config_data.read())
        self.zmqThreadForClient = ZmqThreadForClient(self.config, self.nodes, self.nodes_lock, self.event)
        self.zmqThreadForNode = ZmqThreadForNode(self.config, self.nodes, self.nodes_lock, self.event)

    def run(self):
        self.event.set()
        self.zmqThreadForClient.start()
        self.zmqThreadForNode.start()
