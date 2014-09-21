#
#   Node
#   Connects Overlord socket to tcp://localhost:5556
#

import sys
import zmq

host = "localhost"
port = "5556"

#  Socket to talk to Overlord server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from Overlord server…")
socket.connect("tcp://%s:%s"%(host, port))

key_value = "key_value"
socket.setsockopt_string(zmq.SUBSCRIBE, key_value)

while True:
	string = socket.recv_string()
	print(string)
