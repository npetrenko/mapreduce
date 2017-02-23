import socket
from time import sleep
from config import *
import threading

sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sk.bind(('192.168.0.37', 6000))
sk.listen(10)

class Mapper:

    def __init__(self, addr):
        self.addr = addr

    def get_data(self, sock):
        inst = []
        while True:
            #sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sk.connect(self.addr)
            arr = []
            while True:
                mes = sock.recv(100)
                if not mes:
                    break
                arr.append(mes.decode('utf-8'))
            data = ''.join(arr)
            if data == 'transmission over' or not data:
                break
            inst.append(data)
        sock.close()
        return inst

class Reducer:

    def __init__(self, addr):
        self.addr = addr
        self.load = 0

    def test_connection(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.connect(self.addr)
            sk.close()
            return True
        except:
            sk.close()
            return False


    def send(self, msgs):
        for i, mes in enumerate(msgs):
            self.load += 1
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.connect(self.addr)
            sk.send(mes)
            print(mes)
            sk.close()
        #sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sk.connect(self.addr)
        #sk.send('transmission over')
        #sk.close()

reducers = map(lambda addr: Reducer(addr), reducers)

while True:
    if all(map(lambda reducer: reducer.test_connection(), reducers)):
        print('Reducers connected\nStarting...')
        break
    else:
        print('reducer connection failed - retrying in 5 sec')
        sleep(5)

def get (sock):
    arr = []
    while True:
        ms = sock.recv(100)
        if not ms:
            break
        arr.append(ms)
    return ''.join(arr)

acc = {}
threads = []

def reducestep(line):
    t = threading.Thread(target=actstep, args=(line,))
    threads.append(t)
    t.start()

def actstep(line):
    key, val = line.split(',')
    key = int(key)
    try:
        acc[key] += int(val)
    except:
        acc[key] = int(val)
    return

terminated_mappers = []

tb = False

s = 0
while True:
    arr = []
    print(terminated_mappers)
    if set(mappers).issubset(set(terminated_mappers)):
        tb = True
        break
    mp, addr = sk.accept()
    data = get(mp)
    if ':' in data and '(' in data and ')' in data and '||' in data and 'terminated' in data:
        try:
            # print('data: ', data)
            mapper_host = data.split('(')
            mapper_host = mapper_host[1]
            # print('mh: ', mapper_host)
            mapper_host = mapper_host.split(')')
            mapper_host = mapper_host[0]
            mapper_host = mapper_host.split(':')
            mapper_host[1] = int(mapper_host[1])
            # print('mhf: ', mapper_host)
            if len(mapper_host) != 2:
                Exception('parsing mapper term mesg exception')
            terminated_mappers.append(tuple(mapper_host))
        except:
            Exception('parsing mapper term mesg exception')
    else:
        if data:
            reducestep(data)
            #data = list(map(int, data.split(',')))
            print(data)
            #s += data[1]
            #arr.append(data)
    mp.close()
    #reducers[0].send(['1,' + str(s)])

for t in threads:
    t.join()

for key in acc.keys():
    upl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    upl.connect(('192.168.0.37', 10000))
    upl.send('(' + str(key) + ',' + str(acc[key]) + ')')
    upl.close()

