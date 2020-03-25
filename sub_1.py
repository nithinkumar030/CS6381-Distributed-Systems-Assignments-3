#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#


#
# layer 1 ZMQ functions
#
import zmq
import time
import threading


import os
import sys
from kazoo.client import KazooClient

zk = KazooClient (hosts = "127.0.0.1:2181")

def zmq_client_req_snd_and_recv_from_zk( zmq_addr,value):

    context = zmq.Context()
    #  Socket to talk to server
    socket = context.socket(zmq.REQ)
    socket.connect(zmq_addr)
    print("Sending req_sub...\n")
    socket.send_string(value)
    print("send done")
    time.sleep(1)
    print("Receiving req_sub_rep\n")

    STstr="/topic/"+value+"/sub"
    print(STstr)

    if zk.exists(STstr ):
        print ("/activeSub Side broker indeed exists")

        value,stat = zk.get (STstr, watch=watch_subTopic_change)
        print ("Details of /SubTopic-+value: value = {}, stat = {}".format (value, stat))
        return value
        
    else:
        print ("/SubTopic-+value znode does not exist, why?")
        return -1
    #value=socket.recv_string()
    #print("Received req_sub_rep\n")
    #return value

@zk.DataWatch("/SubTopic-")
def watch_subTopic_change (data, stat):
    print ("\n*********** Inside watch_data_change *********")
    print ("Data changed for znode /activeBrokerPubAddr: data = {}, stat = {}".format (data,stat))
    print ("*********** Leaving watch_data_change *********")


def zmq_client_recv_sub( zmq_addr):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print(zmq_addr)
    socket.connect(zmq_addr)
    print("Receiving sub...\n" )
    value=socket.recv_string()
    print("got sub\n")
    return value


#
#layer 2 ZMQ function
#

@zk.DataWatch("/activeBrokerSubAddr")
def watch_data_change (data, stat):
    print ("\n*********** Inside watch_data_change *********")
    print ("Data changed for znode /activeBrokerSubAddr: data = {}, stat = {}".format (data,stat))
    print ("*********** Leaving watch_data_change *********")


def get_sub_side_broker_ip_address_and_port_from_zk():
   
    if zk.exists ("activeBrokerSubAddr"):
        print ("/activeSub Side broker indeed exists")

        value,stat = zk.get ("/activeBrokerSubAddr", watch=watch_data_change)
        print ("Details of /activeBrokerSubAddr: value = {}, stat = {}".format (value, stat))
        return value
        
    else:
        print ("/activeBrokerSubAddr znode does not exist, why?")
        return -1




topicDict=dict()

def register_sub(topic,subID):
    #let the broker default addr is localhost:6666
    ip_addr_str=get_sub_side_broker_ip_address_and_port_from_zk()
    msgData=[0,1] 
    #ip_addr_str = 'tcp://localhost:6666'
    sub_ipaddr_str=zmq_client_req_snd_and_recv_from_zk(ip_addr_str,topic)
    topicDict[topic]=sub_ipaddr_str
    return

def subscribe(topic):
    sub_ipaddr_str=topicDict[topic]
    
    data=zmq_client_recv_sub(sub_ipaddr_str)
    return data


def retransmit(topic,value):

    hisPath="/History"+"/"+topic
    bmsg=str.encode(str(value))
    zk.create(hisPath,value=bmsg,ephemeral=True,makepath=True)









#
# Test function - later 3 app
#


def sub1_init():

    register_sub('Weather','sub1')


def sub1():
    retrans = True
    count=0
    while(1):

        data1=subscribe('Weather')
        start_time = time.time()
        print("sub1  recvd  timestamp:\n",start_time)
        print("Sub1:\n",data1)
        if retrans == True and count == 2:
            retrans=False
            print("*****************Sub1 requesting Last 2 values to be retransmitted*************")
            retransmit('Weather',2)

        count=count+1
            



        
    #time.sleep(1)
    #data2=subscribe('Stock')
    #print("Sub1:\n",data2)

def sub2_init():
    register_sub('News','sub2')

def sub2():
    while(1):
        data1=subscribe('News')
        start_time = time.time()
        print("sub2  recvd  timestamp:\n",start_time)
        print("Sub2:\n",data1)


        #time.sleep(1)

def sub3_init():
    register_sub('Stock','sub3')
    register_sub('Movie','sub4')

def sub3():
    while(1):
        data1=subscribe('Stock')
        print("Sub3:\n",data1)
        start_time = time.time()
        print("sub3  recvd  timestamp:\n",start_time)
        print("Sub3:\n",data1)
        data1=subscribe('Movie')
        start_time = time.time()
        print("sub3  recvd  timestamp:\n",start_time)
        print("Sub3:\n",data1)




        #time.sleep(1)

def sub4_init():
    register_sub('Sports','sub4')
    


def sub4():
    while(1):

        data1=subscribe('Sports')
        start_time = time.time()
        print("sub4  recvd  timestamp:\n",start_time)
        print("Sub4:\n",data1)
        






def main():
    zk.start()
    print("ZK state :{}",zk.state)



    print("***************Subcriber runnning***********\n")
    sub1_init()
    threading.Thread(target=sub1).start()

    sub2_init()
    threading.Thread(target=sub2).start()

    sub3_init()
    threading.Thread(target=sub3).start()

    #sub4_init()
    #threading.Thread(target=sub4).start()


   


if __name__ == '__main__':
    main()
      



#  Do 10 requests, waiting each time for a response
#for request in range(10):
#    print("Sending request %s …" % request)
#    socket.send(b"Hello")

#    #  Get the reply.
#    message = socket.recv()
#    print("Received reply %s [ %s ]" % (request, message))

