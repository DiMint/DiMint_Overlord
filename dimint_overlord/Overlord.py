import threading, zmq, json
import socket
import time
import traceback
from hashlib import md5
import os, random
import psutil
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import sys
import getopt
import signal

class Hash():
    @staticmethod
    def get_hashed_value(key, hash_range):
        return int(md5(str(key).encode('utf-8')).hexdigest(), 16) % hash_range
    @staticmethod
    def get_node_id():
        return md5(str(time.time()).encode('utf-8')).hexdigest()

class Network():
    @staticmethod
    def get_ip():
        return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
                for s in [socket.socket(socket.AF_INET,
                                        socket.SOCK_DGRAM)]][0][1]

class OverlordStateTask(threading.Thread):
    __zk_manager = None
    __addr = None

    def __init__(self, zk_manager, port_for_client):
        threading.Thread.__init__(self)
        self.__zk_manager = zk_manager
        self.__addr = '{0}:{1}'.format(Network.get_ip(), port_for_client)

    def run(self):
        while True:
            sys.stdout.flush()
            print('OverlordStateTask works')
            if self.__zk_manager is None:
                return
            if not  self.__zk_manager.is_exist('/dimint/overlord/host_list/{0}'.format(self.__addr)):
                return
            msg = self.__zk_manager.get_node_msg('/dimint/overlord/host_list/{0}'.format(self.__addr))
            p = psutil.Process(os.getpid())
            msg['cwd'] = p.cwd()
            msg['name'] = p.name()
            msg['cmdline'] = p.cmdline()
            msg['create_time'] = p.create_time()
            msg['cpu_percent'] = p.cpu_percent()
            msg['memory_percent'] = p.memory_percent()
            msg['memory_info'] = p.memory_info()
            msg['is_running'] = p.is_running()
            self.__zk_manager.set_node_msg('/dimint/overlord/host_list/{0}'.format(self.__addr), msg)
            time.sleep(10)

class ZooKeeperManager():
    __zk = None
    __hash_range = None
    __max_slave_count = None
    __node_list = []

    def __init__(self, zookeeper_hosts, port_for_client, hash_range, max_slave_count):
        self.__zk = KazooClient(zookeeper_hosts)
        self.__zk.start()
        self.__zk.ensure_path('/dimint/overlord/host_list')
        print('ip : ' + Network.get_ip())
        addr = '{0}:{1}'.format(Network.get_ip(), port_for_client)
        host_path = '/dimint/overlord/host_list/' + addr
        if not self.is_exist(host_path):
            self.__zk.create(host_path, b'', ephemeral=True)

        self.__zk.ensure_path('/dimint/node/list')
        self.__zk.ChildrenWatch('/dimint/node/list', self.check_node_is_dead)
        self.__hash_range = hash_range
        self.__max_slave_count = max_slave_count

    def delete_all_node_role(self):
        self.__zk.ensure_path('/dimint/node/role')
        master_node_list = self.__zk.get_children('/dimint/node/list')
        for master_node in master_node_list:
            self.__zk.delete(master_node, recursive=True)

    def get_node_list(self):
        self.__zk.ensure_path('/dimint/node/list')
        self.__node_list = self.__zk.get_children('/dimint/node/list')
        return self.__node_list if isinstance(self.__node_list, list) else []

    def get_overlord_list(self):
        self.__zk.ensure_path('/dimint/overlord/host_list')
        overlord_list = self.__zk.get_children('/dimint/overlord/host_list')
        return overlord_list if isinstance(overlord_list, list) else []

    def get_master_info_list(self):
        self.__zk.ensure_path('/dimint/node/role')
        master_node_list = self.__zk.get_children('/dimint/node/role')
        master_info = {}
        for master in master_node_list:
            info = json.loads(
                self.__zk.get('dimint/node/role/{0}'.format(master))[0].decode('utf-8'))
            master_info[master] = info
        return master_info

    def is_exist(self, path):
        return self.__zk.exists(path)

    def get_identity(self):
        while True:
            hash_value = Hash.get_node_id()
            if not (self.__zk.exists('/dimint/node/list/{0}'.format(hash_value))):
                return str(hash_value)

    def get_node_value(self):
        master_info = self.get_master_info_list()
        value_list = [m['value'] for m in master_info.values()]
        while True:
            hash_value = Hash.get_hashed_value(time.time(), self.__hash_range)
            if not hash_value in value_list:
                return hash_value

    def determine_node_role(self, node_id, node_value, msg, enable):
        role_path = '/dimint/node/role'
        self.__zk.ensure_path(role_path)
        masters = self.__zk.get_children(role_path)
        msg['enabled'] = True
        msg['value'] = node_value
        for master in masters:
            slaves = self.__zk.get_children(os.path.join(role_path, master))
            if len(slaves) < max(1, self.__max_slave_count):
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
        msg['stored_key']=[]
        msg['enabled'] = enable
        self.__zk.create(os.path.join(role_path, node_id),
                         json.dumps(msg).encode('utf-8'))
        return "master", None, None, None

    def select_node(self, key, select_master):
        master = str(self.__select_master_node(key))
        if master is None:
            return [None, None]
        master_path = '/dimint/node/role/{0}'.format(master)
        slaves = self.__zk.get_children(master_path)
        master_info = json.loads(
                self.__zk.get(master_path)[0].decode('utf-8'))
        if not master_info['enabled']:
            return [None, None]
        if (select_master or len(slaves) < 1):
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
        master_info_list = self.get_master_info_list()
        if (len(master_info_list) == 0):
            return None
        master_list = sorted(list(master_info_list.items()), key=lambda tup : int(tup[1]['value']))
        hashed_value = Hash.get_hashed_value(key, self.__hash_range)
        for master in master_list:
            if (hashed_value <= int(master[1]['value'])):
                return master[0]
        return master_list[0][0]

    def __set_master_node_attribute(self, node, attr, value, extra=None):
        node_path = '/dimint/node/role/{0}'.format(node)
        node_info = json.loads(
              self.__zk.get(node_path)[0].decode('utf-8'))
        if extra is not None:
            node_info[attr] = list(set(node_info[attr]) | set([value]))
        else:
            node_info[attr] = value
        self.__zk.set(node_path, json.dumps(node_info).encode('utf-8'))

    def add_key_to_node(self, node, key):
        self.__set_master_node_attribute(node, 'stored_key', key, 0)

    def add_key_list_to_node(self, node, key_list):
        node_path = '/dimint/node/role/{0}'.format(node)
        node_info = json.loads(
            self.__zk.get(node_path)[0].decode('utf-8'))
        node_info['stored_key'] += key_list
        node_info['stored_key'] = list(set(node_info['stored_key']))
        self.__zk.set(node_path, json.dumps(node_info).encode('utf-8'))

    def remove_key_list_from_node(self, node, key_list):
        node_path = '/dimint/node/role/{0}'.format(node)
        node_info = json.loads(
            self.__zk.get(node_path)[0].decode('utf-8'))
        node_info['stored_key'] = list(set(node_info['stored_key']) - set(key_list))

        self.__zk.set(node_path, json.dumps(node_info).encode('utf-8'))

    def enable_node(self, node, enable=True):
        self.__set_master_node_attribute(node, 'enabled', enable)

    def change_node_value(self, node, value):
        self.__set_master_node_attribute(node, 'value', value)

    def get_node_msg(self, node_path):
        try:
            node = self.__zk.get(node_path)
            # node[0] == b''
            # b'' == False
            if not node[0]:
                return {}
            return json.loads(node[0].decode('utf-8'))
        except NoNodeError as e:
            return {}

    def set_node_msg(self, node_path, msg):
        self.__zk.ensure_path(node_path)
        self.__zk.set(node_path, json.dumps(msg).encode('utf-8'))

    def check_node_is_dead(self, node_list):
        # this file is for exclusive lock(?) if other handler doing this job,
        # then delete process should not be executed.
        role_path = '/dimint/node/role'
        handler_working_file = '/dimint/node/list/dead_node_handler_is_working'
        dead_nodes = list(set(self.__node_list) - set(node_list))
        if len(dead_nodes) == 1 and not self.__zk.exists(handler_working_file):
            self.__zk.create(handler_working_file, b'', ephemeral=True)

            for dead_node_id in dead_nodes:
                node_info = self.get_node_info(dead_node_id)
                if node_info.get('role') == 'slave':
                    # if slave node is dead, there is nothing to do.
                    # Just update role information in zookeeper.
                    self.__zk.delete(os.path.join(role_path,
                                                  node_info['master_id'],
                                                  dead_node_id))
                else:
                    master_path = os.path.join(role_path, dead_node_id)
                    try:
                        dominated_master_info = node_info['slaves'][0]
                        other_slaves = node_info['slaves'][1:]

                        write_data = dominated_master_info.copy()
                        del write_data['node_id']
                        del write_data['value']
                        master_data = json.loads(self.__zk.get(master_path)[0].decode('utf-8'))
                        master_data.update(write_data)

                        self.__zk.create(os.path.join(role_path, dominated_master_info['node_id']),
                                         json.dumps(master_data).encode('utf-8'))

                        for other_slave in other_slaves:
                            other_slave_id = other_slave['node_id']
                            del other_slave['node_id']
                            self.__zk.create(
                                os.path.join(role_path,
                                             dominated_master_info['node_id'],
                                             other_slave_id),
                                json.dumps(other_slave).encode('utf-8'))

                        self.__zk.delete(master_path, recursive=True)
                        self.overlord_task_thread.handle_dead_master(
                            dead_node_id, dominated_master_info, other_slaves
                        )
                    except IndexError:
                        # master which doesn't have slave node.
                        # when reached in this block, data dead node stored in
                        # will be lost.
                        self.__zk.delete(master_path)
                        print('Master node without slave node is dead!')

            self.__zk.delete(handler_working_file)
        self.__node_list = node_list

    def get_node_info(self, node_id):
        role_path = '/dimint/node/role'
        for master in self.__zk.get_children(role_path):
            master_path = os.path.join(role_path, master)
            if master == node_id:
                result = {'role': 'master'}
                result.update(json.loads(
                    self.__zk.get(master_path)[0].decode('utf-8')
                ))
                result['slaves'] = [dict(node_id=slave, **json.loads(
                    self.__zk.get(os.path.join(master_path,
                                               slave))[0].decode('utf-8')))
                    for slave in self.__zk.get_children(master_path)]
                return result
            for slave in self.__zk.get_children(
                    os.path.join(role_path, master)):
                if slave == node_id:
                    result = {'role': 'slave', 'master_id': master}
                    result.update(json.loads(self.__zk.get(
                        os.path.join(role_path, master))[0].decode('utf-8')))
                    return result
        raise Exception('Node {0} does not exist in zookeeper'.format(node_id))


class OverlordTask(threading.Thread):
    __zk_manager = None
    __port_for_client = None
    __port_for_node = None
    __zookeeper_hosts = None

    def __init__(self, zk_manager, context, port_for_client, port_for_node, zookeeper_hosts, hash_range):
        threading.Thread.__init__(self)
        self.__zk_manager = zk_manager
        self.__zk_manager.overlord_task_thread = self
        # TODO: temporary code. It must be deleted when you implement healthy check.
        self.__zk_manager.delete_all_node_role()
        self.__context = context
        self.__port_for_client = port_for_client
        self.__port_for_node = port_for_node
        self.__zookeeper_hosts = zookeeper_hosts
        self.__hash_range = hash_range

    def run(self):
        frontend = self.__context.socket(zmq.ROUTER)
        frontend.bind("tcp://*:%s" % self.__port_for_client)
        backend = self.__context.socket(zmq.ROUTER)
        backend.bind("tcp://*:%s" % self.__port_for_node)
        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend, zmq.POLLIN)
        while True:
            sys.stdout.flush()
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
                self.__process_response(ident, response, frontend)
            elif cmd == 'get' or cmd == 'set':
                sender = self.__context.socket(zmq.PUSH)
                master_node, send_addr = self.__zk_manager.select_node(request['key'], cmd=='set')
                if master_node is None:
                    response = {}
                    response['error'] = 'DIMINT_NODE_NOT_AVAILABLE'
                    self.__process_response(ident, response, frontend)
                else:
                    sender.connect(send_addr)
                    sender.send_multipart([ident, msg])
                    if (cmd=='set'):
                        self.__zk_manager.add_key_to_node(master_node, request['key'])
            elif cmd == 'state':
                response = {}
                node_list = self.__zk_manager.get_node_list()
                for node in node_list:
                    msg = self.__zk_manager.get_node_msg('/dimint/node/list/{0}'.format(node))
                    info = self.__zk_manager.get_node_info(node)
                    if not msg is None:
                        msg['node_id'] = node
                        msg['role'] = info['role']
                        if msg['role'] == 'slave':
                            msg['master_node_id'] = info['master_id']
                        if 'state' in response:
                            response['state'].append(msg)
                        else:
                            response['state'] = [msg]
                self.__process_response(ident, response, frontend)
            elif cmd == 'overlord_state':
                response = {}
                overlord_list = self.__zk_manager.get_overlord_list()
                for overlord_id in overlord_list:
                    msg = self.__zk_manager.get_node_msg('/dimint/overlord/host_list/{0}'.format(overlord_id))
                    if not msg is None:
                        msg['overlord_id'] = overlord_id
                        if 'overlord_state' in response:
                            response['overlord_state'].append(msg)
                        else:
                            response['overlord_state'] = [msg]
                self.__process_response(ident, response, frontend)
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
        elif msg.get('cmd') == 'move_key' and backend is not None:
            print(msg)
            self.__zk_manager.remove_key_list_from_node(msg.get('src_id'), msg.get('key_list'))
            self.__zk_manager.add_key_list_to_node(msg.get('target_node_id'), msg.get('key_list'))
            self.__zk_manager.change_node_value(msg.get('src_id'), msg.get('new_src_value'))
            self.__zk_manager.enable_node(msg.get('src_id'))
            self.__zk_manager.enable_node(msg.get('target_node_id'))
        else:
            frontend.send_multipart([ident, response])

    def add_node(self, ident, msg, backend):
        node_id = self.__zk_manager.get_identity()
        rebalance_request = self.__get_rebalance_info(
            node_id, 'tcp://{0}:{1}'.format(msg['ip'], msg['transfer_port']))
        if rebalance_request is None:
            node_value = self.__zk_manager.get_node_value()
        else:
            node_value = rebalance_request['ori_src_value']
        role, master_addr, master_push_addr, master_receive_addr = self.__zk_manager.determine_node_role(
            node_id, node_value, msg, rebalance_request is None)
        response = {
            "node_id": node_id,
            "value": node_value,
            "zookeeper_hosts": self.__zookeeper_hosts,
            "role": role,
        }
        if role == "slave":
            response['master_addr'] = master_push_addr
            response['master_receive_addr'] = master_receive_addr

        backend.send_multipart([ident, json.dumps(response).encode('utf-8')])
        
        if role == "master" and rebalance_request is not None:
            src_id = rebalance_request['src_node_id']
            self.__zk_manager.enable_node(src_id, False)
            sender = self.__context.socket(zmq.PUSH)
            sender.connect(rebalance_request['src_node'])
            sender.send_multipart([src_id.encode('utf-8'),
                json.dumps(rebalance_request).encode('utf-8')])
            print('Rebalance Request to {0} : {1}'.format(src_id, rebalance_request))

    def handle_dead_master(self, dead_master_node_id, new_master_node_info,
                           other_slaves_info):
        s = self.__context.socket(zmq.PUSH)
        s.connect('tcp://{0}:{1}'.format(
            new_master_node_info['ip'],
            new_master_node_info['cmd_receive_port']))
        s.send_multipart([b'', json.dumps({
            'cmd': 'nominate_master',
            'node_id': dead_master_node_id}).encode('utf-8')])
        s.close()

        for other_slave_info in other_slaves_info:
            s = self.__context.socket(zmq.PUSH)
            s.connect('tcp://{0}:{1}'.format(
                other_slave_info['ip'], other_slave_info['cmd_receive_port']))
            s.send_multipart([b'', json.dumps({
                'cmd': 'change_master',
                'master_addr': '{0}:{1}'.format(
                    new_master_node_info['ip'],
                    new_master_node_info['push_to_slave_port']
                )}).encode('utf-8')])
            s.close()

    def __get_rebalance_info(self, target_node_id, target_node_addr):
        while True:
            master_info = self.__zk_manager.get_master_info_list()
            if len(master_info) == 0:
                return None
            enabled_masters = list(filter(lambda x: master_info[x]['enabled'], master_info.keys()))
            if len(enabled_masters) == 0:
                print("add node : waiting for enabled node...")
                time.sleep(2)
                continue
            src_node_id = max(enabled_masters , key=lambda x: len(master_info[x]['stored_key']))
            src_keys = master_info[src_node_id]['stored_key']
            if len(src_keys) == 0:
                return None
            src_value = master_info[src_node_id]['value']
            move_key_list, new_value = OverlordRebalanceTask.select_move_keys(src_keys, [], src_value, src_value, self.__hash_range)            
            request = {}
            request['cmd'] = 'move_key'
            request['key_list'] = move_key_list
            request['new_src_value'] = new_value
            request['src_node_id'] = src_node_id
            request['target_node_id'] = target_node_id
            request['ori_src_value'] = src_value
            request['target_node'] = target_node_addr
            request['src_node'] = 'tcp://{0}:{1}'.format(
                master_info[src_node_id]['ip'], master_info[src_node_id]['cmd_receive_port'])
            return request

class OverlordRebalanceTask(threading.Thread):
    __hash_range = None

    def __init__(self, zk_manager, context, hash_range):
        threading.Thread.__init__(self)
        self.__zk_manager = zk_manager
        self.__context = context
        self.__hash_range = hash_range
        self.__rebalancing = False

    def run(self):
        while True:
            sys.stdout.flush()
            if not self.__rebalancing:
                time.sleep(10)
            request = {}
            request['cmd'] = 'move_key'
            print('check node info for rebalance')
            master_info = self.__zk_manager.get_master_info_list()
            for k, v in master_info.items():
                print ('id: {0}, value: {3}, count: {1}, keys: {2}'.format(k, len(v['stored_key']), v['stored_key'], v['value']))
            
            if len(master_info) < 2:
                self.__rebalancing = False
                continue
            src_id, target_id = self.__select_nodes_ids(master_info)
            if src_id == None:
                self.__rebalancing = False
                continue
            self.__rebalancing = True
            print('src: {0}, target: {1}'.format(src_id, target_id))
            if not master_info[src_id]['enabled'] or not master_info[target_id]['enabled']:
                print ('node is disabled')
                time.sleep(5)
                continue
            self.__zk_manager.enable_node(src_id, False)
            self.__zk_manager.enable_node(target_id, False)
            sender = self.__context.socket(zmq.PUSH)
            src_node = master_info[src_id]
            target_node = master_info[target_id]
            request['key_list'], request['new_src_value'] = self.select_move_keys(
                src_node['stored_key'], target_node['stored_key'],
                src_node['value'], target_node['value'], self.__hash_range)
            request['target_node_id'] = target_id
            request['target_node'] = 'tcp://{0}:{1}'.format(target_node['ip'],
                                                      target_node['transfer_port'])
            sender.connect('tcp://{0}:{1}'.format(src_node['ip'],
                                            src_node['cmd_receive_port']))
            sender.send_multipart([src_id.encode('utf-8'),
                json.dumps(request).encode('utf-8')])
            print('Request to {0} : {1}'.format(src_id, request))

    def __select_nodes_ids(self, master_info):
        index = 0
        src_index = 0
        target_index = 0
        max_gap = 0
        sorted_keys = sorted([(m, int(v['value'])) for m, v in master_info.items()], key=lambda x: x[1])
        max_key_len = len(max(master_info.values(), key=lambda x: len(x['stored_key']))['stored_key'])
        min_key_len = len(min(master_info.values(), key=lambda x: len(x['stored_key']))['stored_key'])
        print('max_stored_key : {0}, min_stored_key : {1}'.format(max_key_len, min_key_len))
        if (max_key_len <= 2*min_key_len):
            return [None, None]

        for i in range(len(sorted_keys)):
            s = i
            t = (i+1) % len(sorted_keys)
            gap = len(master_info[sorted_keys[s][0]]['stored_key']) - len(master_info[sorted_keys[t][0]]['stored_key']) 
            if gap > max_gap:
                src_index = s
                target_index = t
                max_gap = gap
        
        if max_gap == 0:
            return [None, None]

        return [str(sorted_keys[src_index][0]), str(sorted_keys[target_index][0])]

    @staticmethod
    def select_move_keys(src_keys, target_keys, src_value, target_value, hash_range):
        src_hashed = []
        target_hashed = []
        for k in src_keys:
            src_hashed.append((Hash.get_hashed_value(k, hash_range), k))
        for k in target_keys:
            target_hashed.append((Hash.get_hashed_value(k, hash_range), k))

        src_hashed.sort(key=lambda tup: tup[0])
        total_len = len(src_keys) + len(target_keys)
        key_list = []

        if src_value <= target_value and max([k[0] for k in src_hashed]) > target_value:
            lowers = [k for k in src_hashed if k[0] <= src_value] 
            for i in range(int(total_len/2), len(src_keys)):
                offset = (len(lowers) + i) % len(src_keys)
                key_list.append(src_hashed[offset][1])
        else:
            for i in range(int(total_len/2), len(src_keys)):
                key_list.append(src_hashed[i][1])

        offset = total_len // 2 - (((total_len % 2) + 1) % 2) - 1
        if src_value <= target_value and max([k[0] for k in src_hashed]) > target_value:
            uppers = [k for k in src_hashed if k[0] > src_value]
            if len(uppers) <= offset:
                new_offset = offset - len(uppers)
                new_value = src_hashed[new_offset][0]
            else:
                new_value = uppers[offset][0]

        else:
            new_value = src_hashed[offset][0]
        print (key_list)
        print (src_hashed)
        return key_list, new_value

def handler(signum, frame):
    print('Signal handler called with signal', signum)
    sys.exit(0)

class Overlord:
    __overlord_task = None
    __overlord_rebalance_task = None
    __overlord_state_task = None

    def __init__(self):
        pass

    def start_overlord(self, config_path, port_for_client, port_for_node, zookeeper_hosts, hash_range, max_slave_count):
        if not config_path is None:
            with open(config_path, 'r') as config_stream:
                config = json.loads(config_stream.read())
        else:
            config = {}
        if not port_for_client is None:
            config['port_for_client'] = port_for_client
        if not port_for_node is None:
            config['port_for_node'] = port_for_node
        if not zookeeper_hosts is None:
            config['zookeeper_hosts'] = zookeeper_hosts
        if not hash_range is None:
            config['hash_range'] = hash_range
        if not max_slave_count is None:
            config['max_slave_count'] = max_slave_count
        signal.signal(signal.SIGUSR1, handler)
        zk_manager = ZooKeeperManager(config['zookeeper_hosts'], config['port_for_client'], config['hash_range'], config['max_slave_count'])
        context = zmq.Context()
        self.__overlord_task = OverlordTask(zk_manager, context, config['port_for_client'], config['port_for_node'], config['zookeeper_hosts'], config['hash_range'])
        self.__overlord_rebalance_task = OverlordRebalanceTask(zk_manager, context, config['hash_range'])
        self.__overlord_state_task = OverlordStateTask(zk_manager, config['port_for_client'])
        self.__overlord_task.start()
        self.__overlord_rebalance_task.start()
        self.__overlord_state_task.start()

def main(argv = sys.argv[1:]):
    try:
        opts, args = getopt.getopt(argv, '', ['help', 'config_path=', 'port_for_client=', 'port_for_node=', 'zookeeper_hosts=', 'hash_range=', 'max_slave_count='])
    except getopt.GetoptError as e:
        sys.exit(2)
    config_path = os.path.join(os.path.dirname(__file__), 'dimint_overlord.config')
    port_for_client = None
    port_for_node = None
    zookeeper_hosts = None
    hash_range = None
    max_slave_count = None
    for opt, arg in opts:
        if opt == '--help':
            print('dimint_overlord.py --config_path=config_path --port_for_client=port_for_client --port_for_node=port_for_node --zookeeper_hosts=zookeeper_hosts --hash_range=hash_range --max_slave_count=max_slave_count')
            sys.exit(0)
        elif opt == '--config_path':
            config_path = arg
        elif opt == '--port_for_client':
            port_for_client = arg
        elif opt == '--port_for_node':
            port_for_node = arg
        elif opt == '--zookeeper_hosts':
            zookeeper_hosts = arg
        elif opt == '--hash_range':
            hash_range = arg
        elif opt == '--max_slave_count':
            max_slave_count = arg
    overlord = Overlord()
    overlord.start_overlord(config_path, port_for_client, port_for_node, zookeeper_hosts, hash_range, max_slave_count)
    sys.exit(0)

if __name__ == "__main__":
    main()
