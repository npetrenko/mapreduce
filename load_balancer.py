import socket
from time import sleep
import numpy as np
from config import *
import threading


inp = [str(i) for i in range(1000)]
#inp = ['hey', 'there']


class Reducer:

    def __init__(self, addr):
        self.addr = addr
        self.load = 0

    def update_load(self, load):
        self.load += load

    def test_connection(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(self.addr)
            sock.close()
            return True
        except:
            sock.close()
            return False

reducers = map(lambda addr: Reducer(addr), reducers)

sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sk.bind(('192.168.0.37', 7000))
sk.listen(10)

def handler (sock):
    arr = []
    #print('not got there')
    while True:
        mes = sock.recv(100).decode('utf-8')
        #print(mes)
        arr.append(mes)
        if '!' in mes:
            break
    #print('got here')
    #sock.close()
    #sock, addr = sk.accept()
    addload = int(''.join(arr).decode('utf-8')[:-1])
    least_loaded = np.argmin(map(lambda reducer: reducer.load, reducers))
    sock.send(str(least_loaded))
    reducers[least_loaded].update_load(addload)
    print('reducer: %d, addload: %d' % (least_loaded, addload))
    sock.close()

while True:
    if all(map(lambda reducer: reducer.test_connection(), reducers)):
        print('Reducers connected\nStarting...')
        break
    else:
        print('reducer connection failed - retrying in 5 sec')
        sleep(5)

while True:
    sock, addr = sk.accept()
    print('accepted')
    t = threading.Thread(target=handler, args=(sock,))
    t.start()