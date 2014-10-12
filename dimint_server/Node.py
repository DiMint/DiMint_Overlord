import threading, zmq, json

class Node(threading.Thread):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.address = 'tcp://{0}:{1}'.format(self.host, self.port)
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind(self.address)
        self.storage = {}

    def run(self):
        while True:
            message = self.socket.recv()
            result = self.__process(message)
            response = json.dumps(result).encode('utf-8')
            print (result)
            self.socket.send(response)

    def __process(self, message):
        print('Request {0}'.format(message))
        try:
            request = json.loads(message.decode('utf-8'))
            cmd = request['cmd']
            key = request['key']
            response = {}
            if cmd == 'get':
                value = self.storage[key]
            elif cmd == 'set':
                value = request['value']
                self.storage[key] = value
        except:
            value = None
        response['value'] = value
        return response
