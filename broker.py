import logging
import pickle
from queue import SimpleQueue
import socket
import threading
from time import sleep

BUFFER_SIZE = 4096
EOM = b'~END_OF_MESSAGE~'

logging.basicConfig(format= '[%(asctime)s %(levelname)s %(filename)s %(funcName)s - %(threadName)s] %(message)s', level=logging.DEBUG)

class SocketMixin:
    def receive(self, sock, buf):
        while True:
            try:
                chunk = sock.recv(BUFFER_SIZE)
            except OSError as e:
                chunk = b''
                logging.debug('receive error:{}'.format(e))
            if chunk == b'':
                break
            buf += chunk
            if chunk.find(EOM) >= 0:
                break;
        
        msg, sep, buf = buf.partition(EOM)
        return bytes(msg), buf

    def send(self, sock, msg):
        sock.sendall(msg + EOM)

class Broker(SocketMixin):
    def __init__(self, host, port):
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.topics = {}
        self.buf = bytearray()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while True:
                logging.debug('waiting connect...')
                conn, addr = s.accept()
                logging.debug('{} connected'.format(addr))
                threading.Thread(target=self.handle, args=(conn, addr)).start()

    def handle(self, conn, addr):
        with conn:
            action, self.buf = self.receive(conn, self.buf)
            logging.debug('{} - {}'.format(addr, action))
            if action == b'':
                return
            if action != b'sub' and action != b'pub':
                self.send(conn, b'failed')
                logging.debug('{} is not valid'.format(action))
                return
            self.send(conn, b'ok')

            topic_binary, self.buf = self.receive(conn, self.buf)
            logging.debug('topic = {}'.format(topic_binary))
            if topic_binary == b'':
                return
            topic = topic_binary.decode('utf-8')
            self.send(conn, b'ok')
            topic_queue = self.get_topic(topic)

            if action == b'sub':
                self.handle_sub(conn, topic_queue)
            
            if action == b'pub':
                self.handle_pub(conn, topic_queue)
                        

    def get_topic(self, topic):
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = SimpleQueue()
                logging.debug('create a queue for topic: {}'.format(topic))
            return self.topics[topic]       

    def handle_sub(self, conn, topic_queue):
        logging.debug('waiting consumer prepared')
        result, self.buf = self.receive(conn, self.buf)
        if result != b'prepared':
            return

        while True:
            logging.debug('waiting message from queue')
            msg = topic_queue.get()
            logging.debug('get message from queue: {}'.format(msg))
            self.send(conn, msg)
            logging.debug('send message to sub: {}'.format(msg))
            result, self.buf = self.receive(conn, self.buf)
            if result != b'ok':
                break
        
        logging.debug('consumer disconnected')

    def handle_pub(self, conn, topic_queue):
        while True:
            logging.debug('waiting message from producer')
            msg, self.buf = self.receive(conn, self.buf)
            if msg == b'':
                break
            logging.debug('get message from producer: {}'.format(msg))
            topic_queue.put(msg)
            logging.debug('put message in queue:{}'.format(msg))
            self.send(conn, b'ok')
        logging.debug('disconnect with producer')
                
class Subscriber(SocketMixin):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False
        self.buf = bytearray()

    def connect(self, topic):
        self.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.connected = True
        logging.debug('connected to {}:{}'.format(self.host, self.port))
        self.send(self.s, b'sub')
        result, self.buf = self.receive(self.s, self.buf)
        if result != b'ok':
            logging.debug('can not start sub model, result = {}'.format(result))
            return
        logging.debug('start sub model')

        topic_bytes = topic.encode('utf-8')
        self.send(self.s, topic_bytes)
        result, self.buf = self.receive(self.s, self.buf)
        if result != b'ok':
            logging.debug('can not sub to topic, result = {}'.format(result))
            self.connected = False
            return
        
        logging.debug('sub to topic:{}'.format(topic))

    def consume(self, callback):
        if self.connected:
            self.send(self.s, b'prepared')
            msg_buf = b''        
            while True:
                logging.debug('waiting message from broker')
                msg, self.buf = self.receive(self.s, self.buf)
                if msg == b'':
                    break

                logging.debug('get message from broker: {}'.format(msg))
                self.s.sendall(b'ok' + EOM)
                callback(msg)
            
            logging.debug('disconnect with broker')
    
    def close(self):
        if self.connected:
            self.s.shutdown(socket.SHUT_RDWR)
            self.s.close()
            logging.debug('shutdown and close socket')

class Producer(SocketMixin):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False
        self.buf = bytearray()

    def connect(self, topic):
        self.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.connected = True
        logging.debug('connected to {}:{}'.format(self.host, self.port))
        self.send(self.s, b'pub')
        result, self.buf = self.receive(self.s, self.buf)
        if result != b'ok':
            logging.debug('can not start pub model')
            return
        logging.debug('start pub model')

        topic_bytes = topic.encode('utf-8')
        self.send(self.s, topic_bytes)
        result, self.buf = self.receive(self.s, self.buf)
        if result != b'ok':
            logging.debug('can not pub to topic:{}'.format(topic))
            self.connect = False
            return
        
        logging.debug('pub to topic:{}'.format(topic))

    def pub(self, msg):
        if self.connected:
            logging.debug('send message to topic: {}'.format(msg))
            self.send(self.s, msg)
            result, self.buf = self.receive(self.s, self.buf)
            if result != b'ok':
                logging.debug('send message error: {}'.format(msg))
                return False
            
            return True

    def close(self):
        if self.connected:
            self.s.shutdown(socket.SHUT_RDWR)
            self.s.close()
            logging.debug('shutdown and close socket')