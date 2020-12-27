import os 

err_num = 0
batch_size = 10

ok_num = 0
fail_num = 0

for i in range(batch_size):
    os.system('go test -run 2B > /tmp/lab2b')
    with open('/tmp/lab2b','r') as f:
        data=f.read()
        test_num = data.count('Test')
        pass_num = data.count('Pass')
        if test_num==pass_num and test_num==8:
            print('i=%d ok'%i)
            ok_num += 1
        else:
            print('i=%d fail'%i)
            os.system('mv /tmp/lab2b /tmp/lab2b.%d'%i)
            fail_num += 1

print('%d ok'%ok_num)
print('%d fail'%fail_num)
