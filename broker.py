import logging
import pickle
from queue import SimpleQueue
import socket
import threading
from time import sleep

BUFFER_SIZE = 4096
EOM = b'~END_OF_MESSAGE~'

logging.basicConfig(format= '[%(asctime)s %(levelname)s %(filename)s %(funcName)s - %(threadName)s] %(message)s', level=logging.DEBUG)

class Broker:
    def __init__(self, host, port):
        self.lock = threading.Lock()
        self.host = host
        self.port = port
        self.topics = {}

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
            action = conn.recv(BUFFER_SIZE)
            logging.debug('{} - {}'.format(addr, action))
            if action == b'':
                return
            action = action.removesuffix(EOM)
            if action != b'sub' and action != b'pub':
                conn.sendall(b'failed' + EOM)
                logging.debug('{} is not valid'.format(action))
                return
            conn.sendall(b'ok' + EOM)

            topic_binary = conn.recv(BUFFER_SIZE)
            logging.debug('topic = {}'.format(topic_binary))
            if topic_binary == b'':
                return
            topic_binary = topic_binary.removesuffix(EOM)
            topic = topic_binary.decode('utf-8')
            conn.sendall(b'ok' + EOM)
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
        result = conn.recv(BUFFER_SIZE)
        if result != b'prepared' + EOM:
            return

        while True:
            logging.debug('waiting message from queue')
            msg = topic_queue.get()
            logging.debug('get message from queue: {}'.format(msg))
            conn.sendall(pickle.dumps(msg) + EOM)
            logging.debug('send message to sub: {}'.format(msg))
            result = conn.recv(BUFFER_SIZE)
            if result != b'ok' + EOM:
                break
        
        logging.debug('consumer disconnected')

    def handle_pub(self, conn, topic_queue):
        msg_buf = b''
        while True:
            logging.debug('waiting message from producer')
            chunk = conn.recv(BUFFER_SIZE)
            if chunk == b'':
                break
            msg_buf += chunk
            index = msg_buf.rfind(EOM)
            exit = False
            while index == -1:
                chunk += conn.recv(BUFFER_SIZE)
                if chunk == b'':
                    exit = True
                    break
                msg_buf += chunk
                index = msg_buf.rfind(EOM)
            
            if exit:
                break

            msg,sep,msg_buf = msg_buf.partition(EOM)
            logging.debug('get message from producer: {}'.format(msg))
            topic_queue.put(msg)
            conn.sendall(b'ok' + EOM)
        logging.debug('disconnect with producer')
                
class Subscriber:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False

    def connect(self, topic):
        self.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.connected = True
        logging.debug('connected to {}:{}'.format(self.host, self.port))
        self.s.sendall(b'sub' + EOM)
        result = self.s.recv(BUFFER_SIZE)
        if result != b'ok' + EOM:
            logging.debug('can not start sub model, result = {}'.format(result))
            return
        logging.debug('start sub model')

        topic_bytes = topic.encode('utf-8') + EOM
        self.s.sendall(topic_bytes)
        result = self.s.recv(BUFFER_SIZE)
        if result != b'ok' + EOM:
            logging.debug('can not sub to topic, result = {}'.format(result))
            self.connected = False
            return
        
        logging.debug('sub to topic:{}'.format(topic))

    def consume(self, callback):
        if self.connected:
            self.s.sendall(b'prepared' + EOM)
            msg_buf = b''        
            while True:
                logging.debug('waiting message from broker')
                chunk = self.s.recv(BUFFER_SIZE)
                if chunk == b'':
                    break
                msg_buf += chunk
                index = msg_buf.rfind(EOM)
                exit = False
                while index == -1:
                    chunk += self.s.recv(BUFFER_SIZE)
                    if chunk == b'':
                        exit = True
                        break
                    msg_buf += chunk
                    index = msg_buf.rfind(EOM)

                if exit:
                    break

                msg,sep,msg_buf = msg_buf.partition(EOM)
                logging.debug('get message from broker: {}'.format(msg))
                self.s.sendall(b'ok' + EOM)
                callback(msg)
    
    def close(self):
        if self.connected:
            self.s.shutdown(socket.SHUT_RDWR)
            self.s.close()
            logging.debug('shutdown and close socket')

class Producer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False

    def connect(self, topic):
        self.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.connected = True
        logging.debug('connected to {}:{}'.format(self.host, self.port))
        self.s.sendall(b'pub' + EOM)
        result = self.s.recv(BUFFER_SIZE)
        if result != b'ok' + EOM:
            logging.debug('can not start pub model')
            return
        logging.debug('start pub model')

        topic_bytes = topic.encode('utf-8') + EOM
        self.s.sendall(topic_bytes)
        result = self.s.recv(BUFFER_SIZE)
        if result != b'ok' + EOM:
            logging.debug('can not pub to topic:{}'.format(topic))
            self.connect = False
            return
        
        logging.debug('pub to topic:{}'.format(topic))

    def pub(self, msg):
        if self.connected:
            logging.debug('send message to topic: {}'.format(msg))
            self.s.sendall(pickle.dumps(msg) + EOM)
            result = self.s.recv(BUFFER_SIZE)
            if result != b'ok' + EOM:
                logging.debug('send message error: {}'.format(msg))
                return False
            
            return True

    def close(self):
        if self.connected:
            self.s.shutdown(socket.SHUT_RDWR)
            self.s.close()
            logging.debug('shutdown and close socket')