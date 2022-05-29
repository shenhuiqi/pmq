from ast import arg
import queue
from threading import Thread
import threading
from time import sleep


class Broker:
    def __init__(self):
        self.topic = queue.SimpleQueue()

    def put(self, item):
        self.topic.put(item)

    def get(self):
        return self.topic.get()

def sub(broker):
    while True:
        print('{} received: {}'.format(threading.current_thread().name, broker.get()))

def pub(broker):
    n = 0
    while True:
        broker.put(n)
        print('{} send {}'.format(threading.current_thread().name, n))
        n += 1
        sleep(5)

broker = Broker()
Thread(name='sub', target=sub, args=(broker,)).start()
Thread(name='pub', target=pub, args=(broker,)).start()
