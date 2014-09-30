import thread
from Server import Server

class Overlord:
    def __init__(self, port_for_client, port_for_node):
        self.client_mq = []
        self.node_mq = []
        self.server_for_client = Server(port_for_client, self.client_mq)
        self.server_for_node = Server(port_for_node, self.node_mq)

    def run(self):
        self.server_for_client.start()
        self.server_for_node.start()
