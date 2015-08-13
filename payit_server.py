#coding=utf-8
#!/usr/bin/python
########### read config ##################################
import ConfigParser
conf = ConfigParser.ConfigParser()
conf.read('config')

local_server_ip = conf.get('rpc', 'local_server_ip')

########## start Server####################################
import SimpleXMLRPCServer
from payit import Payit

p = Payit()
server = SimpleXMLRPCServer.SimpleXMLRPCServer((local_server_ip, 80))
server.register_instance(p)
server.serve_forever()