#!/usr/bin/python

ECS_ip = '192.168.1.5'

# %% subfunctions: system level:
import socket, fcntl, struct
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
        )[20:24])


ECS_ip = get_ip_address('eth0')

global pool_redis
resulted_pool = list()
# %% func ############################################################
def hdfs_check():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /user/_SUCCESS'
    p_succ = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    hdfs_succ = p_succ.communicate()
    if p_succ.returncode:
        print("#:: No _SUCCESS!")
        exit(2) 


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
    
    if len(a) == 0:
        print('nothing is in hdfs')
        exit(0)
    aa = a.split('\n')

    pool_redis = dict()

    for b in aa:
        bb = b.split('\t')
        pool_redis[bb[0]] = bb[1]
        
    return pool_redis



##################################################################
#first step        
#executed in redis transaction
def redis_update(pipe):
    for element in pool_redis:
        if element != 'profiting':
            print('%s decreased by %s' % (element, pool_redis[element]))
            pipe.incrbyfloat(element, -float(pool_redis[element]))
        else:
            print('%s increased by %s' % (element, pool_redis[element]))
            pipe.incrbyfloat(element, float(pool_redis[element]))

def allUpAliveInvestment_decrease(pool_redis):
    redis_client = redis.Redis(host=ECS_ip, port=6379, db = 0)
    redis_client.transaction(redis_update, pool_redis.keys())

##################################################################
#second step
#find the winning key
def get_match_key(args):
    code = args[0]
    
    args = args[1:]
    
    result = []
    
    i = 0
    while i < len(args):
        product = args[i]
        option = args[i + 1]
        result.append(code + product + option)
        i = i + 2
    
    return result

#whether the element in set contains key
def is_contains_key(key, set):
    for element in set:
        if element in key:
            return True
        
    return False

#modify the alive_m in aliveInvestment
#-1 = dead; 0 = win; >0 = alive
#DO NOT PAY OUT THE SAME MATCH WITH SAME OPTION TWICE 
def aliveInvestment_modified(args):
    redis_client = redis.Redis(host=ECS_ip, port=6379, db = 0)
    
    code = args[0]
    result = get_match_key(args)
    print(result)
    fuzzy_key = '(allUp*' + code + '*aliveInvestment'
    #find all aliveInvestment which contains current match(code)
    hkeys = redis_client.keys(fuzzy_key)
    for hkey in hkeys:
        keys = redis_client.hkeys(hkey)
        for key in keys:
            #if the clause contains winning option
            if is_contains_key(key, result):
                #value(alive_m) is decreased by 1
                hincrby = redis_client.hincrby(hkey, key, -1)
                #add the clause to resulted pool if it is sure to win
                if int(hincrby) == 0:
                    resulted_pool.append(hkey)
                print('hkey:%s with key:%s has been decreased by 1' % (hkey, key))
            # if not winning
            else:
                #value is set to -1 directly
                redis_client.hset(hkey, key, -1)
                flag = True
                values = redis_client.hgetall(hkey).values()
                #if all the clause in pool are dead. if so, add to resulted pool
                for value in values:
                    if int(value) == -1 :
                        continue
                    flag = False
                if flag:
                    resulted_pool.append(hkey)
                
                print('hkey:%s with key:%s has been set to -1' % (hkey, key))
                
####################################################################################
#third step
def update_min_position(code):
    redis_client = redis.Redis(host=ECS_ip, port=6379, db = 0)
    #find all minPosition related with current match
    keys = redis_client.keys('(allUp*' + code + '*minPosition')
    total = 0
    for key in keys:
        aliveInv = redis_client.hgetall(key.replace('minPosition', 'aliveInvestment'))
        #it's a long story...anyway, minimum position will be found and updated
        if len(aliveInv) == 3 ** int(aliveInv.values()[0]):
            keys = aliveInv.keys()
            min_num = 99999999
            
            for key in keys:
                try:
                    position = float(redis_client.hget(key.replace('minPosition', 'position'), key))
                except Exception as e:
                    print('%s does not exsit' % (key.replace('minPosition', 'position'
)))
                if position < min_num:
                    min_num = position
                    
            redis_client.set(key, min_num)

def TotalAliveInvestment_decrease(args):
    redis_client = redis.Redis(host=ECS_ip, port=6379, db = 0)
    
    #single first
    result = get_match_key(args)
    for i in range(len(result) - 1):
        totalPrice = redis_client.get(args[0] + args[2 * i + 1] + 'totalPrice' + args[2 * i + 2])
        totalInvest = redis_client.get(args[0] + args[2 * i + 1] + 'totalInvest')
        try:
            redis_client.incrbyfloat('profiting', float(totalPrice))
            redis_client.incrbyfloat('TotalAliveInvestment', -float(totalInvest))
            redis_client.delete(args[0] + args[2 * i + 1] + 'minPosition')
        except Exception as e:
            print('Single data missed')

    #then allup
    for pool in resulted_pool:
        hkey = pool.replace('aliveInvestment', 'investment')
        totalInvestment = redis_client.hget(hkey, 'totalInvestment')
	try:
            redis_client.incrbyfloat('TotalAliveInvestment', -float(totalInvestment))
        except Exception as e:
            print('Allup data missed')
######################################################################################
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
import sys
from subprocess import Popen,PIPE
from redis.exceptions import WatchError

if __name__ == "__main__":
    args = sys.argv
    print args[1::]
    args = args[1::]
    hdfs_check()
    mr_data = hdfs_read()
    pool_redis = hdfs_parse(mr_data)
    print pool_redis
    allUpAliveInvestment_decrease(pool_redis)
    aliveInvestment_modified(args)
    update_min_position(args[0])
    TotalAliveInvestment_decrease(args)
    #pr.hdfs_reduce_comb(pool_redis)
    #pr.hdfs_rmdir()
