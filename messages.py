from socket_util import read_socket
import struct, json, threading, queue, socket, pickle

log = print

class MessageTooLongError(BaseException):
	pass

class message:
	max_length = 100 * 1024**2		#100 MiB
	prefix_format = '<I'		# <Q for more than 4 gig
	prefix_length = struct.calcsize(prefix_format)
	
	pre_deserialize = None
	pre_serialize = None
	
	def __init__(self, data):
		self.data = data

	@classmethod
	def from_socket(cls, sock, log_raw=None):
		prefix = read_socket(sock, cls.prefix_length)
		length, = struct.unpack_from(cls.prefix_format, prefix)

		if length > cls.max_length:
			raise MessageTooLongError('{length} > {max_length}'.format(length=length, max_length=cls.max_length))

		data = read_socket(sock, length)

		if log_raw:
			log_raw('raw_data', data)

		if cls.pre_deserialize:
			data = cls.pre_deserialize(data)

		return cls(data)

	@classmethod
	def from_bytes(cls, data):
		data_length = len(data)
		if data_length < cls.prefix_length:
			raise EOFError('No more data')
		length, = struct.unpack_from(cls.prefix_format, data)

		if length > cls.max_length:
			raise MessageTooLongError('{length} > {max_length}'.format(length=length, max_length=cls.max_length))

		required_length = cls.prefix_length + length

		if data_length < required_length:
			raise EOFError('No more data')

		data = data[cls.prefix_length:required_length]

		if cls.pre_deserialize:
			data = cls.pre_deserialize(data)

		return cls(data)

	def serialize(self):
		data = self.data

		if self.pre_serialize:
			data = self.pre_serialize(data)

		return struct.pack(self.prefix_format, len(data)) + data


class unicode_message(message):
	@staticmethod
	def pre_serialize(data):
		return bytes(data, 'utf-8')

	@staticmethod
	def pre_deserialize(data):
		return str(data, 'utf-8')

class json_message(message):
	@staticmethod
	def pre_serialize(data):		
		return bytes(json.dumps(data), 'utf-8')

	@staticmethod
	def pre_deserialize(data):
		return json.loads(str(data, 'utf-8'))

class pickle_message(message):
	@staticmethod
	def pre_serialize(data):		
		return pickle.dumps(data)

	@staticmethod
	def pre_deserialize(data):
		return pickle.loads(data)

class socket_message_handler:
	message_type = message
	log_messages = False
	log_connection_state = False
	log_exceptions = False
	log_raw_data = False

	def __init__(self, sock=None):
		self.socket = sock
		self.read_thread = None

	def read_loop(self):
		while True:
			try:
				message = self.message_type.from_socket(self.socket, log if self.log_raw_data else None).data
				if self.log_messages:
					log('message_recv', message)
				self.on_message(message)
			except Exception as e:
				if self.log_exceptions:
					log('exception', e)
				self.on_exception(e)
				break
		if self.log_connection_state:
			log('connection_reset', self.socket)
		self.on_connection_reset()

	def send_message(self, message_data):
		if self.log_messages:
			log('message_send', message_data)
		self.socket.send(self.message_type(message_data).serialize())

	def start(self):
		self.read_thread = threading.Thread(target=self.read_loop)
		self.read_thread.start()


class buffered_socket_message_handler:
	def __init__(self, sock=None):
		self.socket = sock
		self.read_thread = None
		self.write_thread = None
		self.write_queue = queue.Queue()

	def send_message(self, message_data):
		self.write_queue.put(self.message_type(msg).serialize())

	def write_loop(self):
		while True:
			msg = self.write_queue.get()
			if msg is None:
				self.socket.shutdown(socket.SHUT_WR)
				break

			try:
				if self.log_raw_data:
					log('raw_message_send', msg)
				self.socket.send(msg)
			except Exception as e:
				if self.log_exceptions:
					log('exception', e)
				self.on_exception(e)
				break


	def read_loop(self):
		while True:
			try:
				message = self.message_type.from_socket(self.socket, log if self.log_raw_data else None).data
				if self.log_messages:
					log('message_recv', message)
				self.on_message(message)
			except Exception as e:
				if self.log_exceptions:
					log('exception', e)
				self.on_exception(e)
				break
		if self.log_connection_state:
			log('connection_reset', self.socket)
		self.on_connection_reset()
		self.write_queue.put(None)


	def start(self):
		self.read_thread = threading.Thread(target=self.read_loop)
		self.read_thread.start()
		self.write_thread = threading.Thread(target=self.write_loop)
		self.write_thread.start()


class socket_message_reader:
	message_type = message

	def __init__(self, sock=None):
		self.socket = sock

	def __iter__(self):
		while True:
			yield self.message_type.from_socket(self.socket).data
			

	def send_message(self, message_data):		
		self.socket.send(self.message_type(message_data).serialize())

class socket_message_loop:
	#We may want to connect this one to existing queues or even not use queues and use callbacks
	message_type = message
	queue_type = queue.Queue

	def __init__(self, sock):		
		self.socket = sock
		self.read_thread = None
		self.write_thread = None
		self.read_queue = self.queue_type()
		self.write_queue = self.queue_type()

	def read_loop(self):
		while True:
			self.read_queue.put(self.message_type.from_socket(self.socket).data)

	def write_loop(self):
		while True:			
			self.socket.send(self.message_type(self.write_queue.get()).serialize())

	def start(self):
		self.read_thread = threading.Thread(target=self.read_loop)
		self.write_thread = threading.Thread(target=self.write_loop)
		self.read_thread.start()
		self.write_thread.start()
