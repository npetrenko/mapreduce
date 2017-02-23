import socket
from time import sleep, time
from config import *
import threading

n_keys = 2

start = time()
inp = [str((1,i)) for i in range(100)] + [str((2,i)) for i in range(150)]
#inp = ['hey', 'there']

sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sk.bind(('192.168.0.37', 10000))
sk.listen(n_of_reducers)

class Mapper:

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
        sk.send('||terminate||')
        sk.close()

    def send(self, msgs):
        for i, mes in enumerate(msgs):
            self.load += 1
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.connect(self.addr)
            sk.send(mes)
            print(mes)
            sk.close()
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sk.connect(self.addr)
        sk.send('transmission over')
        sk.close()

mappers = map(lambda addr: Mapper(addr), mappers)

while True:
    if all(map(lambda mapper: mapper.test_connection(), mappers)):
        print('Mappers connected\nStarting...')
        break
    else:
        print('mapper connection failed - retrying in 5 sec')
        sleep(5)


block_dim = int(len(inp) / len(mappers))

threads = []
for i, mapper in enumerate(mappers):
    if i != len(mappers) - 1:
        t = threading.Thread(target= mapper.send, args= (inp[i*block_dim: (i + 1) * block_dim],))
        t.start()
        threads.append(t)
    else:
        t = threading.Thread(target=mapper.send, args=(inp[i * block_dim: len(inp)],))
        #mapper.send(inp[i * block_dim: len(inp)])
        t.start()
        threads.append(t)

for t in threads:
    t.join()

for i, mapper in enumerate(mappers):
        mapper.terminate()

for _ in range(n_of_reducers*n_keys):
    sock, addr = sk.accept()
    arr = []
    while True:
        mes = sock.recv(100).decode('utf-8')
        if not mes:
            break
        arr.append(mes)
    data = ''.join(arr)
    print(data)

end = time()
print('Took ' + str(end-start) + ' seconds')