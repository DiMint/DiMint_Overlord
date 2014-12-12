import threading, zmq, json
import socket
import time
import traceback
from hashlib import sha1
import os

from kazoo.client import KazooClient

class Hash():
    @staticmethod
    def get_hashed_value(key):
        return int(sha1(str(key).encode('utf-8')).hexdigest(), 16) % self.__config.get('hash_range')

class ZooKeeperManager():
    __zk = None

    def __init__(self, ip, port):
        self.__zk = KazooClient(self.config.get('zookeeper_hosts', '127.0.0.1:2181'))
        self.__zk.start()
        self.__zk.ensure_path('/dimint/overlord/host_list')
        addr = '{0}:{1}'.format(ip, port)
        host_path = '/dimint/overlord/host_list/' + addr
        if self.is_exist(host_path):
            self.create(host_path, b'', ephemeral=True)

    def delete_all_node_role(self):
        self.__zk.delete('/dimint/node/role', recursive=True)

    def get_node_list(self):
        node_list = self.__zk.get_children('/dimint/node/list')
        return node_list if isinstance(node_list, list) else []

    def get_overlord_list(self):
        overlord_list = self.__zk.get_children('/dimint/overlord/host_list')
        return overlord_list if isinstance(overlord_list, list) else []

    def is_exist(self, path):
        return self.__zk.exists(host_path)

    def create(self, path, msg, ephemeral=False):
        self.__zk.create(path, msg, ephemeral)

    def stop(self):
        self.__zk.stop()

    def get_identity(self):
        while True:
            hash_value = Hash.get_hashed_value(time.time())
            if not (self.__zk.exists('/dimint/node/list/{0}'.format(hash_value))):
                return str(hash_value)

    def determine_node_role(self, node_id, msg):
        role_path = '/dimint/node/role'
        self.__zk.ensure_path(role_path)
        masters = self.__zk.get_children(role_path)
        for master in masters:
            slaves = self.__zk.get_children(os.path.join(role_path, master))
            if len(slaves) < 2:
                master_info = json.loads(
                    self.__zk.get(os.path.join(role_path, master))[0].decode('utf-8'))
                master_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                               master_info['cmd_receive_port'])
                master_push_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                                    master_info['push_to_slave_port'])
                self.__zk.create(os.path.join(role_path, master, node_id),
                                 json.dumps(msg).encode('utf-8'))
                return "slave", master_addr, master_push_addr
        self.__zk.create(os.path.join(role_path, node_id),
                         json.dumps(msg).encode('utf-8'))
        return "master", None, None

class OverlordTask(threading.Thread):
    __config = None
    __port_for_client = None
    __port_for_node = None
    __zk_manager = None
    __nodes = []

    def __init__(self, config, zk_manager):
        threading.Thread.__init__(self)
        self.__config = config
        self.__port_for_client = self.__config['port_for_client']
        self.__port_for_node = self.__config['port_for_node']
        self.__zk_manager = zk_manager
        # TODO: temporary code. It must be deleted when you implement healthy check.
        self.__zk_manager.delete_all_node_role()

    def run(self):
        self.__context = zmq.Context()
        frontend = self.__context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:%s" % self.__port_for_client)
        backend = self.__context.socket(zmq.ROUTER)
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
                response['overlords'] = self.__zk_manager.get_overlord_list()
                response['identity'] = self.__zk_manager.get_identity()
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
        print ('Response {0} id {1}'.format(response, ident))

        if msg.get('cmd') == 'connect' and backend is not None:
            self.add_node(ident, msg, backend)
        else:
            frontend.send_multipart([ident, response])

    def add_node(self, ident, msg, backend):
        node_id = self.__zk_manager.get_identity()
        role, master_addr, master_push_addr = self.__zk_manager.determine_node_role(node_id, msg)
        response = {
            "node_id": node_id,
            "zookeeper_hosts": self.__config.get('zookeeper_hosts'),
            "role": role,
        }
        if role == "slave":
            response['master_addr'] = master_push_addr
            backend.send_multipart([ident, json.dumps(response).encode('utf-8')])

            s = self.__context.socket(zmq.PUSH)
            s.connect(master_addr)
            s.send_json({
                'cmd': 'add_slave',
            })
            print('send add_slave to {0}'.format(master_addr))
        else:
            backend.send_multipart([ident, json.dumps(response).encode('utf-8')])

class Overlord:
    __zk_manager = None

    def __init__(self, config_path=""):
        if (config_path == ""):
            config_path = './dimint_server.config'
        with open(config_path, 'r') as config_data:
            self.config = json.loads(config_data.read())
        self.__zk_manager(self.get_ip(), self.config['port_for_client'])
        self.overlord_task = OverlordTask(self.config, self.__zk_manager)

    def run(self):
        self.overlord_task.start()

    def get_ip(self):
        return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
                for s in [socket.socket(socket.AF_INET,
                                        socket.SOCK_DGRAM)]][0][1]

    def __del__(self):
        self.__zk_manager.stop()

    def __exit__(self, type, value, traceback):
        self.__zk_manager.stop()

