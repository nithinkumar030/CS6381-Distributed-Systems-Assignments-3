#
#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"
#

import threading
import time
import zmq

import os
import sys
from kazoo.client import KazooClient

zk = KazooClient (hosts = "127.0.0.1:2181")



#topicPubIDDict=dict()
topicPubIpAddrDict=dict()
topicSubIpAddrDict=dict()
topicValueDict=dict()
Sema=threading.Semaphore(value=1)



def check_pub_topic_in_zk(topic):

    if zk.exists ("/topic"):
        print ("topic : exists")
        topics = zk.get_children("/topic")
        print(topics)
        if topic in topics:
            if zk.exists ("/topic/"+topic+"/pub"):
                return topic
            else:
                return -1
        else:
            return -1
    else :
        return -1



def check_sub_topic_in_zk(topic):

    if zk.exists ("/topic"):
        print ("topic : exists")
        topics = zk.get_children("/topic")
        print(topics)
        if topic in topics:
            if zk.exists ("/topic/"+topic+"/sub"):
                return topic
            else:
                return -1
        else:
            return -1
    else :
        return -1
        



def zmq_server_recv_pub_req_send_rsp_zk():
    
    port=5555

    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #socket.bind("tcp://*:5555")
        socket.bind("tcp://127.0.0.1:5555")


        #Wait for next request from client
        message = socket.recv_string()
        print("Received pub_req: %s" % message)
        topic=message
        #pubID=message[1]

        #topicPubIDDict[topic]=pubID

        #  Do some 'work'
        #time.sleep(1)
        
        # read from zk the topic node
        #if topic in topicPubIpAddrDict:
        
        chkTopic = check_pub_topic_in_zk(topic)

        if(chkTopic == -1):
            port=port+1
            msg='tcp://127.0.0.1:'+str(port)

            # use zk to store the values ie topic vs pubIP addr
            topicStr="/topic/"+topic+"/pub"
            bmsg=str.encode(msg)
            #topicPubIpAddrDict[topic]=msg
            if zk.exists(topicStr):
                print("zmq_server_recv_sub_req_send_rsp_zk :"+topicStr+" exists")
            else:
                zk.create (topicStr, value=bmsg,ephemeral=True,makepath=True)
        else:
            print("topic"+topic+"exists")

        '''port=port+1
        msg='tcp://127.0.0.1:'+str(port)

        # use zk to store the values ie topic vs pubIP addr
        topicStr="/topic/"+topic+"/pub"
        bmsg=str.encode(msg)
        #topicPubIpAddrDict[topic]=msg
        zk.create (topicStr, value=bmsg, makepath=True)'''


        recv_pub_and_send_sub_init(topic)


def zmq_server_recv_sub_req_send_rsp_zk():
    port=6666

    while True:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        #socket.bind("tcp://*:6666")
        socket.bind("tcp://127.0.0.1:6666")
        

        #Wait for next request from client
        message = socket.recv_string()
        print("Received sub_req: %s" % message)
        topic=message

        chkTopic = check_sub_topic_in_zk(topic)

        if(chkTopic == -1):
            port=port+1
            msg='tcp://127.0.0.1:'+str(port)

            # use zk to store the values ie topic vs pubIP addr
            topicStr="/topic/"+topic+"/sub"
            bmsg=str.encode(msg)
            #topicPubIpAddrDict[topic]=msg
            if zk.exists(topicStr):
                print("zmq_server_recv_sub_req_send_rsp_zk :"+topicStr+" exists")
            else:
                zk.create (topicStr, value=bmsg,ephemeral=True,makepath=True)

        else:
            print("topic"+topic+"exists")

        '''port=port+1
        msg='tcp://127.0.0.1:'+str(port)

        # use zk to store the values ie topic vs pubIP addr
        topicStr="/topic/"+topic+"/sub"
        bmsg=str.encode(msg)
        #topicPubIpAddrDict[topic]=msg
        zk.create (topicStr, value=bmsg,ephemeral=True,makepath=True)'''




def zmq_server_recv_pub_and_send_sub(pub_addr_str,sub_addr_str):
    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.DEALER)
    frontend.bind(pub_addr_str)
    backend.bind(sub_addr_str)

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    # Switch messages between sockets
    #while True:
    socks = dict(poller.poll())

    if socks.get(frontend) == zmq.POLLIN:
        message = frontend.recv_multipart()
        backend.send_multipart(message)

    if socks.get(backend) == zmq.POLLIN:
        message = backend.recv_multipart()
        frontend.send_multipart(message)



def set_pub_broker_ip_addr_port_in_zk():
        print("ZK state :{}",zk.state)
        #if zk.exists("/activeBrokerPubAddr"):
        #    pass
        #else:
        zk.create ("/activeBrokerPubAddr", value=b"tcp://127.0.0.1:5555", ephemeral=True, makepath=True)


def set_sub_broker_ip_addr_port_in_zk():
        print("ZK state :{}",zk.state)
        #if zk.exists("/activeBrokerPubAddr"):
        #    pass
        #else:
        zk.create ("/activeBrokerSubAddr", value=b"tcp://127.0.0.1:6666", ephemeral=True, makepath=True)





    



# 
# Receive req_pub - Create thread 
#
def recv_req_pub_init():
    #set_pub_broker_ip_addr_port_in_zk()
    set_pub_broker_ip_addr_port_in_zk()
    threading.Thread(target=zmq_server_recv_pub_req_send_rsp_zk).start()
    #zmq_server_recv_pub_req_send_rsp()

def recv_req_sub_init():
    set_sub_broker_ip_addr_port_in_zk()
    threading.Thread(target=zmq_server_recv_sub_req_send_rsp_zk).start()


#def recv_pub_init(topic):
#    threading.Thread(target=recv_publish,args=[topic]).start()

#def send_sub_init(topic):
#    threading.Thread(target=send_subscribe,args=[topic]).start()

def recv_pub_and_send_sub_init(topic):
    threading.Thread(target=recv_publish_and_send_subscribe,args=[topic]).start()

    


def recv_publish_and_send_subscribe(topic):
    while(1):

        #read from zk the pubIP addr from topic.
        #pub_addr_str=topicPubIpAddrDict[topic]
        print(topic)
        print(type(topic))
        pubPathStr="/topic/"+topic+"/pub"

        pub_addr_str,stat=zk.get(pubPathStr)
        #read from zk the subIP addr from topic
        #sub_addr_str=topicSubIpAddrDict[topic]

        subPathStr="/topic/"+topic+"/sub"
        

        sub_addr_str,stat=zk.get(subPathStr)

        zmq_server_recv_pub_and_send_sub(pub_addr_str,sub_addr_str)

        #data=zmq_server_recv_pub(ip_addr_str)
        #    print("failed recv_publish")
        #    time.sleep(1)
        #topicValueDict[topic]=data
        time.sleep(1)
   



def recv_publish(topic):

    
    while(1):
        ip_addr_str=topicPubIpAddrDict[topic]
        
        #data=zmq_server_recv_pub(ip_addr_str)
        #    print("failed recv_publish")
        #    time.sleep(1)
        #topicValueDict[topic]=data
        time.sleep(1)


def send_subscribe(topic):
    while(1):
        try:

            ip_addr_str=topicSubIpAddrDict[topic]

            data=topicValueDict[topic]
            
            zmq_server_send_sub(ip_addr_str,data)
            del topicValueDict[topic]
            
            time.sleep(1)

        except Exception as e:
            #print("No data for topic\n")
            pass

        #time.sleep(1)
    


    


def main():
    print("*************Running BROKER ...\n***********")
    zk.start()

    #zk.delete("/election/currIx")



    #Do leader election and if winner then become active broker else stay as standby
    
    if  zk.exists("/election/currIx"):
        i,stat=zk.get("/election/currIx")
        i=int.from_bytes(i,byteorder='little')
        i=i-48
        print("node /election/currIx exists :",i)
    else:
        i=0
        bmsg=str.encode(str(i))
        zk.create("/election/currIx",value=bmsg,makepath=True)
        
    
    while(1):
        if zk.exists("/election/"+str(i)):
            i=i+1
        else:
            newElectionNode="/election/"+str(i)
            zk.create(newElectionNode,ephemeral=True,makepath=True)
            bmsg=str.encode(str(i))
            zk.set("/election/currIx",bmsg)

            print(newElectionNode)

            ix,stat = zk.get("/election/currIx")
            print (ix)

            break
    
    
    while (1):

        time.sleep(1)
        children = zk.get_children("/election")
        children.sort

        for child in children:
            if child == str(i):
                leader = True
                break
            else :
                # watch next higher numbered node
                #zk.get("/election/"+str(i+1))
                leader=False
                break


        if leader == True:
            break;
        

    recv_req_pub_init()

    recv_req_sub_init()
   


if __name__ == '__main__':
    main()
