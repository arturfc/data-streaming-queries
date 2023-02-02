#%%
import time
import sys
import stomp
import ssl

class MsgListener(stomp.ConnectionListener):
    def on_error(self, message):
        print('received an error "%s"' % message.body)
    def on_message(self, frame):
        print('received a message "%s"' % frame.body)

AMQHOST='b-eb4a956e-87a5-4098-b7b6-800582ea541b-1.mq.us-east-1.amazonaws.com'
AMQPORT=61614
AMQUSER='processo-elo'
AMQPASS='ecc7483735f4d26450f0a5f3585ffddd'
TOPICNAME='/topic/processo-elo'

hosts = [(AMQHOST, AMQPORT)]
conn = stomp.Connection(host_and_ports=hosts)
conn.set_listener('stomp_listener', MsgListener())
conn.set_ssl(for_hosts=[(AMQHOST, AMQPORT)], cert_file='cert.pem', key_file='privateKey.key',ssl_version=ssl.PROTOCOL_TLSv1_2)

conn.connect(login=AMQUSER,passcode=AMQPASS, wait=True)
conn.subscribe(destination=TOPICNAME, id=1, ack='auto')
print("Waiting for messages...")
#conn.send(body='testing', destination=TOPICNAME)
while 1: 
  time.sleep(10) 

conn.disconnect()