#coding=utf-8
#!/usr/bin/python
########### read config ##################################
import ConfigParser
conf = ConfigParser.ConfigParser()
conf.read('config')

redis_ip = conf.get('redis', 'redis_ip')
usr = conf.get('db', 'mysql_user')
pwd = conf.get('db', 'mysql_password')
db_ip = conf.get('db', 'mysql_ip')

############ setup logging ###############################
import logging
log_file = '/opt/logs/py_hadoop.log'
    
logging.basicConfig(filename=log_file+'.debug', format='%(asctime)s %(levelname)s :: %(message)s',
                    filemode='w', level=logging.DEBUG)
    
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  #: or ## logger.setLevel(logging.INFO)
logger.addHandler(ch)

############## encapsulate payout ############################################
import sys
import redis
import xmlrpclib
from subprocess import Popen,PIPE
import datetime
import MySQLdb
import thread

redis_client = redis.Redis(host=redis_ip, port=6379, db=0)
resulted_pool = list()
pool_redis = dict()
def redis_update(my_pipeline=redis.Redis.pipeline(redis_client)):
    for element in pool_redis:
        if element != 'profiting':
            logger.info('%s decreased by %s' % (element, pool_redis[element]))
            my_pipeline.incrbyfloat(element, -float(pool_redis[element]))
        else:
            logger.info('%s increased by %s' % (element, pool_redis[element]))
            my_pipeline.incrbyfloat(element, float(pool_redis[element]))

class Payit:
    
    def start_pay(self, args):
        try:
            logger.info(args)
            self.hadoop_pay(args)
            dir_name = args[0]
            for arg in args[1::2]:
                dir_name += arg
            logger.info(dir_name)
            self.hdfs_check(dir_name)
            mr_data = self.hdfs_read(dir_name)
            pool_redis = self.hdfs_parse(mr_data)
            if len(pool_redis) != 0:
                pool_redis = self.sepreate_DML(pool_redis)
                logger.info(pool_redis)
                self.allupaliveinvestment_decrease(pool_redis)
            self.aliveinvestment_modified(args)
            self.update_min_position(args[0])
            self.totalaliveinvestment_decrease(args)
        except Exception:
            self.notifySBC(args, '1')
        self.notifySBC(args, '0')
    
    def call_start_pay(self, args):
        thread.start_new_thread(self.start_pay, (args, ))
        return True
        
    #call MR
    def hadoop_pay(self, args):
        cmd = '/opt/hadoop-2.7.0/bin/hadoop jar /opt/hadoop/payit/payout.jar com.caiex.payout.start.PayOutStart'
        p_pay = Popen(cmd.split() + args , stdin=PIPE, stdout=PIPE)
        p_pay.communicate()
        if p_pay.returncode:
            print("## ERROR: Hadoop payout failed!")
            exit(2)
    
    #check if MR running success
    def hdfs_check(self, dir_name):
        cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /' + dir_name + '/_SUCCESS'
        p_succ = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
        # hdfs_succ = p_succ.communicate()
        p_succ.communicate()
        if p_succ.returncode:
            logger.info("#:: No _SUCCESS!")
            exit(2)
    
    #pull MR result
    def hdfs_read(self, dir_name):
        cmd = '/opt/hadoop-2.7.0/bin/hdfs dfs -cat /' + dir_name + '/part-r-00000'
        p_part = Popen(cmd.split(), stdin=PIPE, stdout=PIPE)
        mr_result = p_part.communicate()
        mr_data0 = None
        if p_part.returncode:
            logger.info("#:: No MapReduced!")
            exit(2)
        else:
            mr_data0 = mr_result[0]
    
        return mr_data0
    
    #put MR result into a dict
    def hdfs_parse(self, mr_data0):
        a = mr_data0.rstrip('\n')
    
        if len(a) == 0:
            logger.info('nothing is in hdfs')
            return {}
        else:
            aa = a.split('\n')
        
            pool_redis0 = dict()
        
            for b in aa:
                bb = b.split('\t')
                pool_redis0[bb[0]] = bb[1]
        
            return pool_redis0
        
    ############################################################
    # before first step
    # execute DMLs and seprate them from pool_redis
    def sepreate_DML(self, pool_redis0):
        conn = MySQLdb.connect(host=db_ip, user=usr, passwd=pwd, db="caiex",charset="utf8")
        cu = conn.cursor()
        
        for element in pool_redis0.keys():
            if 'sid' in element:
                status_code = element[-1]
                sql = pool_redis0[element]
                #last match to payout
                if status_code == '1':
                    time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                    param = (time)
                    cu.execute(sql, param)
                #payout but not the last match
                elif status_code == '2':
                    cu.execute(sql)
                #dead end
                elif status_code == '3':
                    time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                    param = (time)
                    cu.execute(sql, param)
                
                del pool_redis0[element]
        
        conn.commit()
        cu.close()
        conn.close()
        
        return pool_redis0
    
    ################################################################
    # first step
    # executed in redis transaction
    
    
    def allupaliveinvestment_decrease(self, pool_redis0):
        redis_client.transaction(redis_update, pool_redis0.keys())
    
    ##################################################################
    # second step
    # find the winning key
    def get_match_key(self, args0):
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
    def is_contains_key(self, key0, set0):
        for element in set0:
            if element in key0:
                return True
    
        return False
    
    
    # modify the alive_m in aliveInvestment
    # -1 = dead; 0 = win; >0 = alive
    # DO NOT PAY OUT THE SAME MATCH WITH SAME OPTION TWICE
    def aliveinvestment_modified(self, args0):
        code = args0[0]
        result = self.get_match_key(args0)
        logger.info(result)
        fuzzy_key = '(allUp*' + code + '*aliveInvestment'
        # find all aliveInvestment which contains current match(code)
        hkeys = redis_client.keys(fuzzy_key)
        for hkey in hkeys:
            keys = redis_client.hkeys(hkey)
            for key in keys:
                # if the clause contains winning option and is not dead
                if self.is_contains_key(key, result) and redis_client.hget(hkey, key) != '-1':
                    # value(alive_m) is decreased by 1
                    hincrby = redis_client.hincrby(hkey, key, -1)
                    # add the clause to resulted pool if it is sure to win
                    if int(hincrby) == 0:
                        resulted_pool.append(hkey)
                    logger.info('hkey:%s with key:%s has been decreased by 1' % (hkey, key))
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
    
                    logger.info('hkey:%s with key:%s has been set to -1' % (hkey, key))
    
    
    ####################################################################################
    # third step
    def update_min_position(self, code):
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
                        logger.info(e)
                        logger.info('%s %s do not exist' % (key1.replace('minPosition', 'position', key2)))
                    if position < min_num:
                        min_num = position
    
                redis_client.set(key1, min_num)
    
    
    def totalaliveinvestment_decrease(self, args0):
        # single first
        result = self.get_match_key(args0)
        for i in range(len(result)):
            # total_price = redis_client.get(args0[0] + args0[2 * i + 1] + 'totalPrice' + args0[2 * i + 2])
            try:
                # remove current single minimum position from risk investment
                minPosKey = args0[0] + args0[2 * i + 1] + 'minPosition'
                redis_client.delete(minPosKey)
                logger.info('Single minPosition has been deleted:' + minPosKey)
                
                total_invest = redis_client.get(args0[0] + args0[2 * i + 1] + 'totalInvest')
                redis_client.incrbyfloat('TotalAliveInvestment', -float(total_invest))
                redis_client.incrbyfloat('DeadInvestment', float(total_invest))
                logger.info('TotalAliveInvestment decreased by ' + total_invest + ' from ' + args0[0] + args0[2 * i + 1])
            except Exception as e:
                logger.info(e)
                logger.info(minPosKey + 'Single data missed')
    
        # then all up
        logger.info(resulted_pool)
        for pool in resulted_pool:
            hkey = pool.replace('aliveInvestment', 'investment')
            try:
                # remove current all up minimum position from risk investment
                minPosKey = pool.replace('aliveInvestment', 'minPosition')
                redis_client.delete(minPosKey)
                logger.info('Allup minPosition has been deleted' + minPosKey)
                
                total_investment = redis_client.hget(hkey, 'totalInvestment')
                redis_client.incrbyfloat('TotalAliveInvestment', -float(total_investment))
                redis_client.incrbyfloat('DeadInvestment', float(total_investment))
                logger.info('TotalAliveInvestment decreased by ' + total_investment + ' from ' + hkey)
            except Exception as e:
                logger.info(e)
                logger.info(minPosKey + 'Allup data missed')
            
    ###########################################################################
    #return the result to SBC
    def notifySBC(self, args, result):
        params = 'match_code=' + args[0] + '&&'
        products = ''
        for arg in args[1::2]:
            products += arg + '_'
        params += 'product=' + products[:-1:] + '&&result=' + result + '&&payCancel=0' 
        logger.info(params)
        server = xmlrpclib.ServerProxy('http://' + conf.get('rpc', 'remote_server_ip') + ':8080/SBC/matchSend/sbcPay.do?' + params)
        try:
            server.sbcPay()
        except BaseException:
            logger.info('call success')
            
    #test rpc
    def test_rpc(self, args):
        return args[0]
