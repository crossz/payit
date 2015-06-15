#!/opt/anaconda/bin/python


ECS_ip='192.168.1.5'


#def demo():
    ## demo for output of 'ls -l'
    
    #p_ls = Popen(["hdfs", "dfs", "-ls", "/user"], stdin=PIPE, stdout=PIPE)
    #hdfs_ls = p_ls.communicate()
    #out=hdfs_ls[0]
    
    ## or:
    #from subprocess import check_output
    #out = check_output(["ls", "-l"])
    
def hdfs_check():
    p_succ = Popen(["hdfs", "dfs", "-cat", "/user/_SUCCESS"], stdin=PIPE, stdout=PIPE)
    hdfs_succ = p_succ.communicate()
    if p_succ.returncode:
        print("#:: No _SUCCESS!")
        exit(2) 
        #print("##: test exit")

def hdfs_read():
    p_part = Popen(["hdfs", "dfs", "-cat", "/user/part-r-00000"], stdin=PIPE, stdout=PIPE)
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
        #print(b)

    return pool_redis


def hdfs_reduce(pool_redis):
    cu_r = redis.Redis(host=ECS_ip, port=6379, db = 0)
    c = pool_redis
    for cc in c:
        r_key = cc[0]
        r_val = cc[1]

        try:
            r_val_new = float(cu_r.get(r_key)) - float(r_val)
            cu_r.set(r_key, r_val_new)

            print('%s : %s -> %f' % (r_key, r_val, r_val_new))
        except Exception as e:
            print('## WARN: no value for this key %s #########################' % (r_key))



def hdfs_rmdir():
    p_rm = Popen(["sudo", "/opt/hadoop-2.7.0/bin/hdfs", "dfs", "-rm", "-r", "/user"], stdin=PIPE, stdout=PIPE)
    hdfs_rm = p_rm.communicate()
    if p_rm.returncode:
        print('#:: Data cleared!')
        exit(3)
    
    





import redis
from subprocess import Popen,PIPE
if __name__ == "__main__":
    
    
    hdfs_check()
    mr_data = hdfs_read()
    pool_redis = hdfs_parse(mr_data)
    hdfs_reduce(pool_redis)
    hdfs_rmdir()


