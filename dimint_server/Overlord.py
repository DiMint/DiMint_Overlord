import threading, zmq, json

class ZmqThreadForClient(threading.Thread):
    def __init__(self, config, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_client']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % self.port)

    def run(self):
        while True:
            message = self.socket.recv()
            result = self.__process_request(message)
            response = json.dumps(result).encode('utf-8')
            print (response)
            self.socket.send(response)
            #self.nodes_lock.acquire()
            # use nodes
            #print(self.nodes)
            #self.nodes_lock.release()

    def __process_request(self, message):
        print('Node : Request {0}'.format(message))
        response = {}
        try:
            request = json.loads(message.decode('utf-8'))
            if (request['cmd'] == 'get_overloads'):
                response['status'] = 'ok'
                response['overloads'] = self.config['overloads']
            else:
                response['status'] = 'not implemented'
        except:
            response['status'] = 'invalid command'
        return response

class ZmqThreadForNode(threading.Thread):
    def __init__(self, config, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_node']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % self.port)

    def run(self):
        while True:
            message = self.socket.recv()
            self.socket.send(message)
            print("qwer", message)
            self.nodes_lock.acquire()
            # modify nodes
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            self.nodes.append((context, socket))
            self.nodes_lock.release()

class Overlord:
    def __init__(self, config_path=""):
        self.nodes = []
        self.nodes_lock = threading.Lock()
        if (config_path == ""):
            config_path = './dimint_server.config'
        with open(config_path, 'r') as config_data:
            self.config = json.loads(config_data.read())
        self.zmqThreadForClient = ZmqThreadForClient(self.config, self.nodes, self.nodes_lock)
        self.zmqThreadForNode = ZmqThreadForNode(self.config, self.nodes, self.nodes_lock)

    def run(self):
        self.zmqThreadForClient.start()
        self.zmqThreadForNode.start()
