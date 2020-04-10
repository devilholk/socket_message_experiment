from messages import message

class unicode_message(message):
	@staticmethod
	def pre_serialize(data):
		return bytes(data, 'utf-8')

	@staticmethod
	def pre_deserialize(data):
		return str(data, 'utf-8')

msg = unicode_message('Räksmörgås')


serialized_data = msg.serialize()

print(serialized_data)

print(unicode_message.from_bytes(serialized_data).data)