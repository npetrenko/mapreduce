import socket
from time import sleep
from config import *
import threading

sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = 4000
sk.bind(('192.168.0.37', port))
sk.listen(1)

host = socket.gethostbyname(socket.gethostname())

class LoadBalancer:

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
            if data == '||terminate||':
                return '||terminate||'
            inst.append(data)
        sock.close()
        return inst



class LoadBalancerReduce:

    def __init__(self, addr):
        self.addr = addr

    def test_connection(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.connect(self.addr)
            sk.close()
            return True
        except:
            sk.close()
            return False

    def get_reducer(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.connect(self.addr)
        arr = []
        while True:
            mes = sk.recv(100)
            if not mes:
                break
            arr.append(mes.decode('utf-8'))
        addnport = ''.join(arr)
        ret = addnport.split(',')
        sk.close()
        return ret[0], int(ret[1])


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

    def terminate(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.connect(self.addr)
        sk.send('||host (' + host + ':' + str(port) + ') terminated||')
        #print(mes)
        sk.close()

    def send(self, msgs):
        for i, mes in enumerate(msgs):
            self.load += 1
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.connect(self.addr)
            sk.send(mes)
            #print(mes)
            sk.close()
        #sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #sk.connect(self.addr)
        #sk.send('transmission over')
        #sk.close()

balancer_reduce = LoadBalancerReduce(balancer_reduce)
balancer = LoadBalancer(balancer)
reducers = map(lambda addr: Reducer(addr), reducers)


while True:
    if all(map(lambda reducer: reducer.test_connection(), reducers)):
        print('Reducers connected\nStarting...')
        break
    else:
        print('reducer connection failed - retrying in 5 sec')
        sleep(5)

def get_least_loaded(load = 1):
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.connect(load_balancer)
    sk.send(str(load) + '!')
    #sk.close()

    #sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #sk.connect(load_balancer)
    arr = []
    while True:
        mes = sk.recv(100).decode('utf-8')
        if not mes:
            break
        arr.append(mes.decode('utf-8'))
    sk.close()
    return int(''.join(arr))

def mapstep(line):
    line = line.split('(')[1].split(')')[0]
    return line


threads = []
break_flag = False

def getnsend(lb):
    global break_flag
    if not break_flag:
        data = balancer.get_data(lb)
        if data == '||terminate||':
            for reducer in reducers:
                reducer.terminate()
            break_flag = True
        ll = get_least_loaded()
        reducers[ll].send(map(mapstep, data))

while True:
    if break_flag:
        break
    lb, addr = sk.accept()
    t = threading.Thread(target=getnsend, args=(lb,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()



