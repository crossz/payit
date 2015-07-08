#!/usr/bin/python

def hadoop_pay(args):

    cmd = '/opt/hadoop-2.7.0/bin/hadoop jar /opt/hadoop/payit/payout.jar com.caiex.payout.start.PayOutStart'
    
    p_pay = Popen(cmd.split() + args , stdin=PIPE, stdout=PIPE)
    p_pay.communicate()
    if p_pay.returncode:
        print("## ERROR: Hadoop payout failed!")
        exit(2) 


import sys
from subprocess import Popen,PIPE
if __name__ == "__main__":
    args0 = sys.argv
    args = args0[1:]
    hadoop_pay(args)


