import socket
import threading
from time import sleep

def run():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        
        while True:
            conn, addr = s.accept()
            handle_noblock(conn, addr)

def handle_noblock(conn, addr):
    t = threading.Thread(target=handle, args=(conn, addr))
    t.start()

def handle(conn, addr):
    global n
    with conn:
        print('Connected by', addr)
        while True:
            sleep(10)
            msg = 'server data:{}'.format(n)
            n += 1
            try:
                conn.sendall(msg.encode('utf-8'))
                print('send to {}:{}'.format(addr, msg))
            except OSError as e:
                print('send to {} error: {}'.format(addr, e))
                break

            try:
                data = conn.recv(1024)
                print('received from {}:{}'.format(addr, data))
            except OSError as e:
                print('received from {} error: {}'.format(addr, e))
                break
                
                
if __name__ == '__main__':
    HOST = ''
    PORT = 10000
    n = 1
    run()