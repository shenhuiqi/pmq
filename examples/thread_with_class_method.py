import threading


class Test:
    def __init__(self):
        self.text = 'abc'

    def test(self, msg):
        print('instance method: {}, {}'.format(self.text, msg))

    def class_method(msg):
        print('class method:{}'.format(msg))

t = Test()
threading.Thread(target=t.test, args=('123',)).start()
threading.Thread(target=Test.class_method, args=('455',)).start()