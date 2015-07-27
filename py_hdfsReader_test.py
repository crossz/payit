#!/usr/bin/python
# -*- coding: utf-8 -*-

# %% configuration #############################
import ConfigParser
conf=ConfigParser.ConfigParser()
conf.read('config.properties')
redis_url = conf.get('service_ip','redisURL')
# mysql_url = conf.get('mysqlURL')
ECS_ip = redis_url

#: logging setting
import logging
log_file = '/opt/logs/py_hadoop.log'

logging.basicConfig(filename=log_file+'.debug', format='%(asctime)s %(levelname)s :: %(message)s',
                    filemode='w', level=logging.DEBUG)

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  #: or ## logger.setLevel(logging.INFO)
logger.addHandler(ch)



# %% func ######################################
def hdfs_check():
    logger.info(ECS_ip)





if __name__ == "__main__":
    hdfs_check()
