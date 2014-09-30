import threading
import zmq

class ZmqThreadForClient(threading.Thread):
    def __init__(self, port, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.port = port
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)

    def run(self):
        while True:
            message = self.socket.recv()
            self.socket.send(message)
            print("asdf", message)
            self.nodes_lock.acquire()
            # use nodes
            print(self.nodes)
            self.nodes_lock.release()

class ZmqThreadForNode(threading.Thread):
    def __init__(self, port, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.port = port
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)

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
    def __init__(self, port_for_client, port_for_node):
        self.nodes = []
        self.nodes_lock = threading.Lock()
        self.zmqThreadForClient = ZmqThreadForClient(port_for_client, self.nodes, self.nodes_lock)
        self.zmqThreadForNode = ZmqThreadForNode(port_for_node, self.nodes, self.nodes_lock)

    def run(self):
        self.zmqThreadForClient.start()
        self.zmqThreadForNode.start()
