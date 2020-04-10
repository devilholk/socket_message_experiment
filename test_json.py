from messages import json_message, pickle_message, unicode_message

data = dict(hello = 'world')

print(pickle_message(data).serialize())
print()
print(json_message(data).serialize())
#print()
#print(unicode_message(data).serialize())