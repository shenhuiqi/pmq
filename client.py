import socket
from time import sleep

HOST = '127.0.0.1'
PORT = 10000

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    n = 1
    while True:
        try:
            data = s.recv(1024)
            print('received:' + repr(data))
        except OSError as e:
            print('received error:{}'.format(e))

        try:
            msg = 'client data:{}'.format(n)
            n = n + 1
            s.sendall(msg.encode('utf-8'))
            print('send:{}'.format(msg))
        except OSError as e:
            print('send error:{}'.format(e))