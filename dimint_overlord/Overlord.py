import threading, zmq, json
import socket
import time
import traceback
from hashlib import md5
import os, random

from kazoo.client import KazooClient

config = None

class Hash():
    @staticmethod
    def get_hashed_value(key):
        global config
        hash_range = config.get('hash_range')
        print('hash_range : ' + str(hash_range))
        return int(md5(str(key).encode('utf-8')).hexdigest(), 16) % hash_range

class Network():
    @staticmethod
    def get_ip():
        return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
                for s in [socket.socket(socket.AF_INET,
                                        socket.SOCK_DGRAM)]][0][1]

class ZooKeeperManager():
    __zk = None

    def __init__(self):
        global config
        zookeeper_hosts = config.get('zookeeper_hosts', '127.0.0.1:2181')
        port_for_client = config['port_for_client']
        print('zookeeper_hosts : ' + zookeeper_hosts)
        print('port_for_client : ' + str(port_for_client))
        self.__zk = KazooClient(zookeeper_hosts)
        self.__zk.start()
        self.__zk.ensure_path('/dimint/overlord/host_list')
        print('ip : ' + Network.get_ip())
        addr = '{0}:{1}'.format(Network.get_ip(), port_for_client)
        host_path = '/dimint/overlord/host_list/' + addr
        if not self.is_exist(host_path):
            self.__zk.create(host_path, b'', ephemeral=True)

    def delete_all_node_role(self):
        self.__zk.delete('/dimint/node/role', recursive=True)

    def get_node_list(self):
        node_list = self.__zk.get_children('/dimint/node/list')
        return node_list if isinstance(node_list, list) else []

    def get_overlord_list(self):
        overlord_list = self.__zk.get_children('/dimint/overlord/host_list')
        return overlord_list if isinstance(overlord_list, list) else []

    def is_exist(self, path):
        return self.__zk.exists(path)

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
        msg['enabled'] = True
        for master in masters:
            slaves = self.__zk.get_children(os.path.join(role_path, master))
            if len(slaves) < 2:
                master_info = json.loads(
                    self.__zk.get(os.path.join(role_path, master))[0].decode('utf-8'))
                master_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                               master_info['cmd_receive_port'])
                master_push_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                                    master_info['push_to_slave_port'])
                master_receive_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                                             master_info['receive_slave_port'])
                self.__zk.create(os.path.join(role_path, master, node_id),
                                 json.dumps(msg).encode('utf-8'))
                return "slave", master_addr, master_push_addr, master_receive_addr
        msg['stored_key']={}
        self.__zk.create(os.path.join(role_path, node_id),
                         json.dumps(msg).encode('utf-8'))
        return "master", None, None, None

    def select_node(self, key, select_master):
        master = str(self.__select_master_node(key))
        if master is None:
            return None
        master_path = '/dimint/node/role/{0}'.format(master)
        slaves = self.__zk.get_children(master_path)
        if (select_master or len(slaves) < 1):
            master_info = json.loads(
                self.__zk.get(master_path)[0].decode('utf-8'))
            master_addr = 'tcp://{0}:{1}'.format(master_info['ip'],
                                               master_info['cmd_receive_port'])
            return [master, master_addr]
        else:
            slave = random.choice(slaves)
            slave_path = '/dimint/node/role/{0}/{1}'.format(master, slave)
            slave_info = json.loads(
                self.__zk.get(slave_path)[0].decode('utf-8'))
            slave_addr = 'tcp://{0}:{1}'.format(slave_info['ip'],
                                              slave_info['cmd_receive_port'])
            return [master, slave_addr]

    def __select_master_node(self, key):
        master_string_list = self.__zk.get_children('/dimint/node/role')
        if (len(master_string_list) == 0):
            return None
        master_list = sorted([int(m) for m in master_string_list])
        hashed_value = Hash.get_hashed_value(key)
        for master in master_list:
            if (hashed_value <= master):
                return master
        return master_list[0]

    def __set_master_node_attribute(self, node, attr, value, extra=None):
        node_path = 'dimint/node/role/{0}'.format(node)
        node_info = json.loads(
              self.__zk.get(node_path)[0].decode('utf-8'))
        if extra is not None:
            node_info[attr][value]=extra
        else:
            node_info[attr] = value
        self.__zk.set(node_path, json.dumps(node_info).encode('utf-8'))

    def add_key_to_node(self, node, key):
        self.__set_master_node_attribute(node, 'stored_key', key, 0)

    def enable_node(self, node, enable=True):
        self.__set_master_node_attribute(node, 'enabled', enable)

class OverlordTask(threading.Thread):
    __zk_manager = None
    __nodes = []

    def __init__(self, zk_manager):
        threading.Thread.__init__(self)
        self.__zk_manager = zk_manager
        # TODO: temporary code. It must be deleted when you implement healthy check.
        self.__zk_manager.delete_all_node_role()

    def run(self):
        global config
        port_for_client = config['port_for_client']
        port_for_node = config['port_for_node']
        print('port_for_client : ' + str(port_for_client))
        print('port_for_node : ' + str(port_for_node))
        self.__context = zmq.Context()
        frontend = self.__context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:%s" % port_for_client)
        backend = self.__context.socket(zmq.ROUTER)
        backend.bind("tcp://*:%s" % port_for_node)
        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend, zmq.POLLIN)
        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                ident, msg = frontend.recv_multipart()
                self.__process_request(ident, msg, frontend, backend)
            if backend in sockets:
                result = backend.recv_multipart()

                self.__process_response(result[-2], result[-1], frontend, backend)
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
                master_node, send_addr = self.__zk_manager.select_node(request['key'], cmd=='set')
                sender.connect(send_addr)
                sender.send_multipart([ident, msg])
                if (cmd=='set'):
                    self.__zk_manager.add_key_to_node(master_node, request['key'])
            else:
                response = {}
                response['error'] = 'DIMINT_NOT_FOUND'
                self.__process_response(ident, response, frontend)
        except Exception as e:
            response = {}
            response['error'] = 'DIMINT_PARSE_ERROR'
            traceback.print_exc()
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
        global config
        zookeeper_hosts = config.get('zookeeper_hosts')
        print('zookeeper_hosts : ' + zookeeper_hosts)
        node_id = self.__zk_manager.get_identity()
        role, master_addr, master_push_addr, master_receive_addr = self.__zk_manager.determine_node_role(node_id, msg)
        response = {
            "node_id": node_id,
            "zookeeper_hosts": zookeeper_hosts,
            "role": role,
        }
        if role == "slave":
            response['master_addr'] = master_push_addr
            response['master_receive_addr'] = master_receive_addr

            s = self.__context.socket(zmq.PUSH)
            s.connect(master_addr)
            cmd = {'cmd': 'add_slave'}
            s.send_multipart([ident, json.dumps(cmd).encode('utf-8')])
        else:
            backend.send_multipart([ident, json.dumps(response).encode('utf-8')])

class Overlord:
    __zk_manager = None

    def __init__(self, config_path=""):
        if (config_path == ""):
            config_path = './dimint_server.config'
        with open(config_path, 'r') as config_data:
            global config
            config = json.loads(config_data.read())
        self.__zk_manager = ZooKeeperManager()
        self.overlord_task = OverlordTask(self.__zk_manager)

    def run(self):
        self.overlord_task.start()

    def __del__(self):
        self.__zk_manager.stop()

    def __exit__(self, type, value, traceback):
        self.__zk_manager.stop()
