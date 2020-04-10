from messages import socket_message_handler, pickle_message
from socket_util import socketpair_tcp
import time

class socket_message_handler_demo(socket_message_handler):
	message_type = pickle_message
	log_messages = False
	log_connection_state = True
	log_exceptions = True
	
	def on_message(self, msg):
		print(f'Vi fick meddelande: {msg}')

sender, reciever = socketpair_tcp()

rec_handler = socket_message_handler_demo(reciever)
send_handler = socket_message_handler_demo(sender)

rec_handler.start()

while 1:
	time.sleep(0.5)
	send_handler.send_message(
		{'hello': 420}
	)
