import os 
import threading
from queue import Queue

err_num = 0
batch_size = 1024
thread_num = 32

lock = threading.Lock()
q = Queue()
ok_num = 0
fail_num = 0

def test(i):
    global ok_num
    global fail_num
    os.system('go test -run 2B > /tmp/lab2b.%d'%i)
    with open('/tmp/lab2b.%d'%i,'r') as f:
        data=f.read()
        test_num = data.count('Test (2B):')
        pass_num = data.count('Passed')
        lock.acquire()
        if test_num==pass_num and test_num==8:
            print('i=%d ok'%i)
            ok_num += 1
        else:
            print('i=%d fail'%i)
            #os.system('mv /tmp/lab2b /tmp/lab2b.%d'%i)
            fail_num += 1
        lock.release()

def thread_loop():
    while True:
        i = q.get()
        if i is None:
            break 
        test(i)

t_list = [threading.Thread(target=thread_loop) for i in range(thread_num)]

for t in t_list:
    t.start()

for i in range(batch_size):
    q.put(i) 

for i in range(thread_num):
    q.put(None)

for t in t_list:
    t.join()

print('%d ok'%ok_num)
print('%d fail'%fail_num)
