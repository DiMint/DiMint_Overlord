import threading, zmq, json
import socket
import time

from kazoo.client import KazooClient

active_request = {}
active_result = {}


class ZmqThreadForClient(threading.Thread):
    def __init__(self, config, nodes, nodes_lock, event, zk):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_client']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % self.port)
        self.event = event
        self.zk = zk

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
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            if cmd == 'get_overlords':
                response['overlords'] = self.get_overlord_list()
            elif cmd == 'get' or cmd == 'set':
                active_request = message
                # self.event.clear()
                # self.event.wait()
                response['value'] = active_result['value']
            else:
                response['error'] = 'DIMINT_NOT_FOUND'
        except Exception as e:
            response['error'] = 'DIMINT_PARSE_ERROR'
        return response

    def get_overlord_list(self):
        overlord_list = self.zk.get_children('/dimint/overlord/host_list')
        return overlord_list if isinstance(overlord_list, list) else []


class ZmqThreadForNode(threading.Thread):
    def __init__(self, config, nodes, nodes_lock, event, zk):
        threading.Thread.__init__(self)
        self.config = config
        self.port = self.config['port_for_node']
        self.nodes = nodes
        self.nodes_lock = nodes_lock
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://{0}:{1}".format('127.0.0.1', self.port))
        self.event = event
        self.zk = zk

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
        self.register_to_zk()
        self.zmqThreadForClient = ZmqThreadForClient(self.config, self.nodes, self.nodes_lock, self.event, self.zk)
        self.zmqThreadForClient.daemon = True
        self.zmqThreadForNode = ZmqThreadForNode(self.config, self.nodes, self.nodes_lock, self.event, self.zk)
        self.zmqThreadForNode.daemon = True

    def register_to_zk(self):
        self.zk = KazooClient(self.config.get('zookeeper_hosts',
                                              '127.0.0.1:2181'))
        self.zk.start()
        self.zk.ensure_path('/dimint/overlord/host_list')

        addr = '{0}:{1}'.format(self.get_ip(), self.config['port_for_client'])
        host_path = '/dimint/overlord/host_list/' + addr
        if not self.zk.exists(host_path):
            self.zk.create(host_path, b'', ephemeral=True)

    def run(self):
        self.event.set()
        self.zmqThreadForClient.start()
        self.zmqThreadForNode.start()
        while True:
            time.sleep(1)

    def get_ip(self):
        return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
                for s in [socket.socket(socket.AF_INET,
                                        socket.SOCK_DGRAM)]][0][1]

    def __del__(self):
        self.zk.stop()

    def __exit__(self, type, value, traceback):
        self.zk.stop()
