#!/bin/sh

date > /opt/logs/py_hadoop.log
#/opt/anaconda/bin/python /home/ubuntu/payit/py_payout.py 2011 1 X[-1] /opt/logs/py_hadoop.log >> /opt/logs/py_hadoop.log 2>&1
/opt/anaconda/bin/python /home/ubuntu/payit/py_payout.py $1 $2 $3 /opt/logs/py_hadoop.log >> /opt/logs/py_hadoop.log 2>&1
/opt/anaconda/bin/python /home/ubuntu/payit/py_hdfsReader.py >> /opt/logs/py_hadoop.log 2>&1

