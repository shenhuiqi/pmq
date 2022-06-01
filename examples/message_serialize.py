import pickle


class Message:
    def __init__(self):
        self.no = 1
        self.name = '上海'

msg = Message()
print(msg.__dict__)
msg_serialized = pickle.dumps(msg)
print(msg_serialized)
msg_unsierialized = pickle.loads(msg_serialized)
print(msg_unsierialized.__dict__)