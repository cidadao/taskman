import sys
import json
import socket
import threading
import select
import time

class TcpClient(object):
	def __init__(self):
		self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self._alive = False

	def connect(self, hostname, port):
		self._client.connect((hostname, port))
		self._thread_listener = threading.Thread(target = self._listener)
		self._thread_listener.setDaemon(True)
		self._alive = True
		self._thread_listener.start()

	def disconnect(self):
		self._alive = False;
		self._client.close()

	def _listener(self):
		while self._alive:
			input,output,err = select.select([self._client],[],[]) 
			for rd in input: 
				if self._alive is True:
					data = rd.recv(1024)
					if not data:
						break
					print "rx:", data

	def send(self, data):
		self._client.send(data)


if __name__ == "__main__":

	client = TcpClient()
	client.connect("localhost", 1234)

	print "Write some stuff and press enter."

	jsonrpc_id = 0
	while True:
		inputText = raw_input()
		args = inputText.split(' ')
		print args
		if len(args) == 0:
			print "Invalid data"
		else:
			method = args[0]
			#params = args[1:]
			params = {}
			for arg in args[1:]:
				named_arg = arg.split('=')
				if len(named_arg) == 2:
					params.update({named_arg[0] : named_arg[1]})
				else:
					print "Invalid param:", named_arg

			if inputText == "quit":
				break
			else:
				data_to_send = json.dumps({
						"method": method,
						"params": params,
						"id": jsonrpc_id
					}
					)
				print data_to_send
				client.send(data_to_send)
				jsonrpc_id += 1

	client.disconnect()
	print "done!"