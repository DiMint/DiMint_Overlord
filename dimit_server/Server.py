import threading
import zmq

class Server(threading.Thread):
    def __init__(self, port, mq):
        threading.Thread.__init__(self)
        self.port = port
        self.mq = mq
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://*:%s" % port)

    def run(self):
        while True:
            message = self.socket.recv()
            print "qwer", message()
            threadLock.acquire()
            mq.append(message)
            threadLock.release()
