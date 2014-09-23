#
#   Overlord server
#   Binds PUB socket to tcp://*:5556
#

import zmq
from random import randrange

port = "5556"

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:%s" % port)

key_value = "key_value"

while True:
	key = randrange(1000)
	value = randrange(-1000, 0)
	socket.send_string("%s %s %s" % (key_value, key, value))