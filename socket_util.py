import socket

def socketpair_tcp():
	L = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	B = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	L.bind(('', 0))
	L.listen(0)
	B.connect(L.getsockname())
	A, B_addr = L.accept()
	L.close()
	return A, B


def read_socket(sock, required_length):
	buf = sock.recv(required_length)
	remaining_length = required_length - len(buf)
	while remaining_length:
		data = sock.recv(remaining_length)
		if not data:
			raise ConnectionResetError()
		buf += data
		remaining_length = required_length - len(buf)

	return buf
