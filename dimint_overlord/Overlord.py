import threading, zmq, json
import socket
import time
import traceback
from hashlib import sha1

from kazoo.client import KazooClient

class OverlordTask(threading.Thread):
    __config = None
    __port_for_client = None
    __port_for_node = None
    __zk = None
    __nodes = []

    def __init__(self, config, zk):
        threading.Thread.__init__(self)
        self.__config = config
        self.__port_for_client = self.__config['port_for_client']
        self.__port_for_node = self.__config['port_for_node']
        self.__zk = zk

    def run(self):
        self.__context = zmq.Context()
        frontend = self.__context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:%s" % self.__port_for_client)
        backend = self.__context.socket(zmq.DEALER)
        backend.bind("tcp://*:%s" % self.__port_for_node)
        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend, zmq.POLLIN)
        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                ident, msg = frontend.recv_multipart()
                self.__process_request(ident, msg, frontend, backend)
            if backend in sockets:
                ident, msg = backend.recv_multipart()
                self.__process_response(ident, msg, frontend, backend)
        frontend.close()
        backend.close()
        self.__context.term()

    def __process_request(self, ident, msg, frontend, backend):
        print('Request {0} id {1}'.format(msg, ident))
        try:
            request = json.loads(msg.decode('utf-8'))
            cmd = request['cmd']
            if cmd == 'get_overlords':
                response = {}
                response['overlords'] = self.get_overlord_list()
                response['identity'] = self.__get_identity()
                self.__process_response(ident, response, frontend)
            elif cmd == 'get' or cmd == 'set':
                sender = self.__context.socket(zmq.PUSH)
                sender.connect("tcp://127.0.0.1:15556")
                sender.send_multipart([ident, msg])
            else:
                response = {}
                response['error'] = 'DIMINT_NOT_FOUND'
                self.__process_response(ident, response, frontend)
        except Exception as e:
            response = {}
            response['error'] = 'DIMINT_PARSE_ERROR'
            self.__process_response(ident, response, frontend)

    def __process_response(self, ident, msg, frontend, backend=None):
        msg = json.loads(msg.decode('utf-8')) if isinstance(msg, bytes) else msg
        response = json.dumps(msg).encode('utf-8')
        print ('Response {0}'.format(response))

        if msg.get('cmd') == 'connect' and backend is not None:
            self.add_node(ident, msg, backend)
        else:
            frontend.send_multipart([ident, response])

    def add_node(self, ident, msg, backend):
        # TODO: determine node's role
        response = json.dumps({
            "node_id": self.__get_identity(),
            "zookeeper_hosts": self.__config.get('zookeeper_hosts'),
            "role": "master",  # temp
        }).encode('utf-8')
        backend.send_multipart([ident, response])

    def __get_identity(self):
        while True:
            hash_value = self.__get_hashed_value(time.time())
            if not (self.__zk.exists('/dimint/node/list/{0}'.format(hash_value))):
                return hash_value

    def get_overlord_list(self):
        overlord_list = self.__zk.get_children('/dimint/overlord/host_list')
        return overlord_list if isinstance(overlord_list, list) else []
   
    def __get_hashed_value(self, key):
        return int(sha1(str(key).encode('utf-8')).hexdigest(), 16) % self.__config.get('hash_range')

    def __get_node_list(self):
        node_list = self.__zk.get_children('/dimint/node/list')
        return node_list if isinstance(node_list, list) else []

class Overlord:
    def __init__(self, config_path=""):
        if (config_path == ""):
            config_path = './dimint_server.config'
        with open(config_path, 'r') as config_data:
            self.config = json.loads(config_data.read())
        self.register_to_zk()
        self.overlord_task = OverlordTask(self.config, self.zk)
        self.overlord_task.daemon = True

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
        self.overlord_task.start()
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

