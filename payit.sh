#/bin/sh

init(){
    # redis
    redis-cli set 2011HADallUpBankerAliveInvestX 36.66
    
    # mysql 
    mysql -ucaiex -p12345678 << EOF
    update caiex.order_ticket_detail set status=3, alive_m=1 where sid=9455;
    update caiex.order_ticket_detail set status=3, alive_m=1 where sid=9456;
    update caiex.order_ticket_detail set status=3, alive_m=1 where sid=9458;
    update caiex.order_ticket_detail set status=3, alive_m=1 where sid=9460;
EOF
}


check(){
    echo '---- after '$1' ---------------------'
    echo '~$ HDFS data:'
    hdfs dfs -cat /user/part-r-00000
    echo '~$ Redis value:'
    redis-cli keys 2011HADallUpBankerAliveInvestX | xargs redis-cli get
    echo '----------------------------------'
}


mapreduce(){
    sudo python py_payout.py 2011 1 X[-1]
}

redisupdate(){
    sudo python payit/py_hdfsReader.py
 
}




state="init"
echo '#### 1) '$state
eval $state
check $state


state="mapreduce"
echo '#### 2) '$state
eval $state
check $state


state="redisupdate"
echo '#### 3) '$state
eval $state
check $state

