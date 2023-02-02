#%%

import paho.mqtt.client as mqttClient

import time
import ssl

context = ssl.create_default_context()

Connected = False 
broker_address= "b-eb4a956e-87a5-4098-b7b6-800582ea541b-1.mq.us-east-1.amazonaws.com" # No ssl://
port = 8883
user = "processo-elo"
password = "ecc7483735f4d26450f0a5f3585ffddd"

def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("Connected to broker")
        global Connected                #Use global variable
        Connected = True                #Signal connection  
        client.subscribe("processo-elo")
    else:
        print("Connection failed")

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

client = mqttClient.Client("asdsad") #create new instance
client.username_pw_set(user, password=password) #set username and password
client.on_connect=on_connect
client.on_message=on_message
client.tls_set_context(context=context)
client.connect(broker_address, port=port)
client.loop_start()

while True:
    time.sleep(10)

#client.publish("test/data", "This is my test msg 123.")
#time.sleep(10)

print("leaving")
