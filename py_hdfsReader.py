#!/opt/anaconda/bin/python

# %% subfunctions: system level:
import socket, fcntl, struct
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
        )[20:24])


# %% func
def hdfs_check():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /user/_SUCCESS'
    p_succ = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    hdfs_succ = p_succ.communicate()
    if p_succ.returncode:
        print("#:: No _SUCCESS!")
        exit(2) 
        #print("##: test exit")

def hdfs_read():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /user/part-r-00000'
    p_part = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    mr_result = p_part.communicate()
    if p_part.returncode:
        print("#:: No MapReduced!")
        exit(2) 
    else:
        mr_data = mr_result[0]
    
    return mr_data    

def hdfs_parse(mr_data):
    a = mr_data.rstrip('\n')
    aa=a.split('\n')

    pool_redis = list()

    for b in aa:
        bb = b.split('\t')
        pool_redis.append(bb)

    return pool_redis


def hdfs_reduce_inv(pool_redis):
    ECS_ip = get_ip_address('eth0')

    cu_r = redis.Redis(host=ECS_ip, port=6379, db = 0)
    c = pool_redis
    for cc in c:
        r_key = cc[0]
        r_val = cc[1]

        try:
        #if r_val_old:
            r_val_old = cu_r.get(r_key)

            r_val_new = float(r_val_old) - float(r_val)
            r_val_new = round(r_val_new,2)
            #r_val_new = "{0:.4f}".format(r_val_new)
            cu_r.set(r_key, r_val_new)
            print('%s : %s -> %f' % (r_key, r_val_old, r_val_new))
            print(r_val_new)
        except Exception as e:
        #else:
            print('#### WARN: no value for this key %s ####' % (r_key))
            print e


def hdfs_reduce_comb(pool_redis):
    #ECS_ip='192.168.1.5'
    cu_r = redis.Redis(host=ECS_ip, port=6379, db = 0)
    d = pool_redis
    
    



    for cc in c:
        r_key = cc[0]
        r_val = cc[1]

        try:
        #if r_val_old:

            r_val_old = cu_r.get(r_key)

            r_val_new = float(r_val_old) - float(r_val)
            cu_r.set(r_key, r_val_new)
            print('%s : %s -> %f' % (r_key, r_val_old, r_val_new))
        except Exception as e:
        #else:
            print('#### WARN: no value for this key %s ####' % (r_key))
            print e


def hdfs_rmdir():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -rm -r /user'
    p_rm = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    hdfs_rm = p_rm.communicate()
    if p_rm.returncode:
        print('##: Remove mapreduce result failed!')
        exit(3)
    else:
        print('##: Data cleared!')





import redis
from subprocess import Popen,PIPE
import py_hdfsReader as pr

if __name__ == "__main__":

    pr.hdfs_check()
    mr_data = pr.hdfs_read()
    pool_redis = pr.hdfs_parse(mr_data)
    print pool_redis
    pr.hdfs_reduce_inv(pool_redis)
    #pr.hdfs_reduce_comb(pool_redis)
    #pr.hdfs_rmdir()

