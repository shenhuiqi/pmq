import sys
sys.path.append('.')

from broker import Subscriber

def consume(msg):
    print(msg)

sub = Subscriber('127.0.0.1', 10000)
sub.connect('913101120000000001-001')
sub.consume(consume)