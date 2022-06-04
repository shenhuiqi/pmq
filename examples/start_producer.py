import sys
sys.path.append('.')

from broker import Producer

producer = Producer('127.0.0.1', 10000)
producer.connect('913101120000000001-001')
producer.pub(b'hello1')
#producer.pub('hello2')

#producer.connect('913101120000000001-002')
#producer.pub('world1')
#producer.pub('world2')