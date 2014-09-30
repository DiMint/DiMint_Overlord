import threading
import zmq

class ZmqThreadForClient(threading.Thread):
    def __init__(self, port, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.port = port
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)

    def run(self):
        while True:
            message = self.socket.recv()
            print "qwer", message
            nodes_lock.acquire()
            # use nodes
            nodes_lock.release()

class ZmqThreadForNode(threading.Thread):
    def __init__(self, port, nodes, nodes_lock):
        threading.Thread.__init__(self)
        self.port = port
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)

    def run(self):
        while True:
            message = self.socket.recv()
            print "qwer", message
            nodes_lock.acquire()
            # modify nodes
            nodes_lock.release()

class Overlord:
    def __init__(self, port_for_client, port_for_node):
        self.nodes_lock = threading.Lock()
        self.nodes = []
        self.zmqThreadForClient = ZmqThreadForClient(port_for_client, self.nodes, self.nodes_lock)
        self.zmqThreadForNode = ZmqThreadForNode(port_for_node, self.nodes, self.nodes_lock)

    def run(self):
        self.zmqThreadForClient.start()
        self.zmqThreadForNode.start()
