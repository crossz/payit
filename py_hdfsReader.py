#!/usr/bin/python

##: temp setting:
#ECS_ip = '192.168.1.5'

import socket
import fcntl
import struct


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])


ECS_ip = get_ip_address('eth0')


######################### 


import redis
import sys
from subprocess import Popen, PIPE
# from redis.exceptions import WatchError

redis_client = redis.Redis(host=ECS_ip, port=6379, db=0)
resulted_pool = list()


# %% func ############################################################


def hdfs_check():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /' + dir_name + '/_SUCCESS'
    p_succ = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    # hdfs_succ = p_succ.communicate()
    p_succ.communicate()
    if p_succ.returncode:
        print("#:: No _SUCCESS!")
        exit(2)


def hdfs_read():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /' + dir_name + '/part-r-00000'
    p_part = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    mr_result = p_part.communicate()
    mr_data0 = None
    if p_part.returncode:
        print("#:: No MapReduced!")
        exit(2)
    else:
        mr_data0 = mr_result[0]

    return mr_data0


def hdfs_parse(mr_data0):
    a = mr_data0.rstrip('\n')

    if len(a) == 0:
        print('nothing is in hdfs')
        exit(0)
    aa = a.split('\n')

    pool_redis0 = dict()

    for b in aa:
        bb = b.split('\t')
        pool_redis0[bb[0]] = bb[1]

    return pool_redis0


##################################################################
# first step
# executed in redis transaction

def redis_update(my_pipeline=redis.Redis.pipeline(redis_client)):
    for element in pool_redis:
        if element != 'profiting':
            print('%s decreased by %s' % (element, pool_redis[element]))
            my_pipeline.incrbyfloat(element, -float(pool_redis[element]))
        else:
            print('%s increased by %s' % (element, pool_redis[element]))
            my_pipeline.incrbyfloat(element, float(pool_redis[element]))


def allupaliveinvestment_decrease(pool_redis0):
    redis_client.transaction(redis_update, pool_redis0.keys())


##################################################################
# second step
# find the winning key
def get_match_key(args0):
    code = args0[0]

    args0 = args0[1:]

    result = []

    i = 0
    while i < len(args0):
        product = args0[i]
        option = args0[i + 1]
        result.append(code + product + option)
        i += 2

    return result


# whether the element in set contains key
def is_contains_key(key0, set0):
    for element in set0:
        if element in key0:
            return True

    return False


# modify the alive_m in aliveInvestment
# -1 = dead; 0 = win; >0 = alive
# DO NOT PAY OUT THE SAME MATCH WITH SAME OPTION TWICE
def aliveinvestment_modified(args0):
    code = args0[0]
    result = get_match_key(args0)
    print(result)
    fuzzy_key = '(allUp*' + code + '*aliveInvestment'
    # find all aliveInvestment which contains current match(code)
    hkeys = redis_client.keys(fuzzy_key)
    for hkey in hkeys:
        keys = redis_client.hkeys(hkey)
        for key in keys:
            # if the clause contains winning option
            if is_contains_key(key, result):
                # value(alive_m) is decreased by 1
                hincrby = redis_client.hincrby(hkey, key, -1)
                # add the clause to resulted pool if it is sure to win
                if int(hincrby) == 0:
                    resulted_pool.append(hkey)
                print('hkey:%s with key:%s has been decreased by 1' % (hkey, key))
            # if not winning
            else:
                # value is set to -1 directly
                redis_client.hset(hkey, key, -1)
                flag = True
                values = redis_client.hgetall(hkey).values()
                # if all the clause in pool are dead. if so, add to resulted pool
                for value in values:
                    if int(value) == -1:
                        continue
                    flag = False
                if flag:
                    resulted_pool.append(hkey)

                print('hkey:%s with key:%s has been set to -1' % (hkey, key))


####################################################################################
# third step
def update_min_position(code):
    # find all minPosition related with current match
    keys1 = redis_client.keys('(allUp*' + code + '*minPosition')

    for key1 in keys1:
        alive_inv = redis_client.hgetall(key1.replace('minPosition', 'aliveInvestment'))
        # it's a long story...anyway, minimum position will be found and updated
        if len(alive_inv) == 3 ** int(alive_inv.values()[0]):
            keys2 = alive_inv.keys()
            min_num = 99999999

            for key2 in keys2:
                try:
                    position = float(redis_client.hget(key1.replace('minPosition', 'position'), key2))
                    
                except Exception as e:
                    print(e)
                    print('%s %s do not exist' % (key1.replace('minPosition', 'position', key2)))
                if position < min_num:
                    min_num = position

            redis_client.set(key1, min_num)


def totalaliveinvestment_decrease(args0):
    # single first
    result = get_match_key(args0)
    for i in range(len(result) - 1):
        total_price = redis_client.get(args0[0] + args0[2 * i + 1] + 'totalPrice' + args0[2 * i + 2])
        total_invest = redis_client.get(args0[0] + args0[2 * i + 1] + 'totalInvest')
        try:
            redis_client.incrbyfloat('TotalAliveInvestment', -float(total_invest))
            # remove current single minimum position from risk investment
            redis_client.delete(args0[0] + args0[2 * i + 1] + 'minPosition')
        except Exception as e:
            print(e)
            print('Single data missed')

    # then all up
    print resulted_pool
    for pool in resulted_pool:
        hkey = pool.replace('aliveInvestment', 'investment')
        total_investment = redis_client.hget(hkey, 'totalInvestment')
        try:
            redis_client.incrbyfloat('TotalAliveInvestment', -float(total_investment))
            # remove current all up minimum position from risk investment
            redis_client.delete(pool.replace('aliveInvestment', 'minPosition'))
        except Exception as e:
            print(e)
            print('Allup data missed')


######################################################################################
def hdfs_rmdir():
    cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -rm -r /user'
    p_rm = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
    # hdfs_rm = p_rm.communicate()
    p_rm.communicate()
    if p_rm.returncode:
        print('##: Remove mapreduce result failed!')
        exit(3)
    else:
        print('##: Data cleared!')


if __name__ == "__main__":
    args = sys.argv
    print args[1::]
    args = args[1::]
    dir_name = args[0]
    i = 1
    while i < len(args):
	dir_name += args[i]
	i += 2 
    print dir_name
    hdfs_check()
    mr_data = hdfs_read()
    pool_redis = hdfs_parse(mr_data)
    print pool_redis
    allupaliveinvestment_decrease(pool_redis)
    aliveinvestment_modified(args)
    update_min_position(args[0])
    totalaliveinvestment_decrease(args)
    # pr.hdfs_rmdir()
