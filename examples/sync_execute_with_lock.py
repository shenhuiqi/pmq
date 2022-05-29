
from concurrent.futures import thread
import threading


class Account:
    def __init__(self, no, balance):
        self.lock = threading.Lock()
        self.no = no
        self.balance = balance
    
    def withdraw(self, amount):
        with self.lock:
            if self.balance >= amount:
                print('{} withdraw {}'.format(threading.current_thread().name, amount))
                self.balance -= amount
                print('{} balance is {}'.format(threading.current_thread().name, self.balance))
            else:
                print('{} can not withdraw {} < {}'.format(threading.current_thread().name, amount, self.balance))

acct = Account('001', 1000)

threading.Thread(name='A', target=acct.withdraw, args=(800,)).start()
threading.Thread(name='B', target=acct.withdraw, args=(800,)).start()