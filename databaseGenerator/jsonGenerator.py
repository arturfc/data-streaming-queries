#%% 
#Populating data into mongoDB
import json
import os
import random
import time
import names
import secrets
import string
from datetime import datetime, date
from pymongo import MongoClient
from random import randrange
from datetime import timedelta

IS_POPULATED = False
SAMPLE_SIZE = 100
STREAMING_SAMPLE_SIZE_RANGE = 4
START_DATE = '2022-10-1 1:30 PM'
END_DATE = '2022-12-5 1:30 PM'

def randomDateList(start, end):
    """
    This function will return a sorted list of random datetime between two datetime objects.
    parameters:
    ------------
    start: datetime.datetime
    end: datetime.datetime

    return: datetime.datetime
    """
    dateList = []
    delta = end - start
    for i in range(SAMPLE_SIZE):

        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        dateList.append(start + timedelta(seconds=random_second))

    return dateList

def getIdCode():
    """
    create random id code containing 12 characters
    ------------
    return: string
    """
    alphabet = string.ascii_letters + string.digits
    id_code = ''.join(secrets.choice(alphabet) for i in range(12))
    return id_code

def getSymptoms(symptomsList):
    '''
        this function returns the list of possible symptoms
        parameters:
        -------------
        symptomsList: list

        return: list
    '''
    symptomsAcquired = []
    numberOfSymptoms = randrange(1, 5)
    symptomsList_aux = symptomsList.copy()

    for i in range(numberOfSymptoms):
        z = randrange(0, len(symptomsList_aux))
        symptomsAcquired.append(symptomsList_aux[z])
        symptomsList_aux.pop(z) 
     
    return symptomsAcquired

client = MongoClient("mongodb://127.0.0.1:27017")
mydb = client["local"]
myCollection = mydb["chatBot_feed"]

startDate = datetime.strptime(START_DATE, '%Y-%m-%d %I:%M %p')
endDate = datetime.strptime(END_DATE, '%Y-%m-%d %I:%M %p')

dateList = randomDateList(startDate, endDate)
dateList.sort()

client_id = ['9yuroy1XaHlD','LPwtuW9Uk5lO', 'eTskRctMUqqv']
client_name = ["SUS", "Ipseng", "CASSI"]
channel = ['WhatsApp','Telegram', 'Site']
symptoms = ['dor_de_cabeca','tontura', 'febre','tosse','diarreia']
diagnostic = ['covid','sinusite','influenza','adenovirus']
services = ['exame_covid_online', 'marcar_consulta_presencial','marcar_consulta_online']
boolean=[True, False]

if not IS_POPULATED:
    jsonRegister = []
    for i in range(SAMPLE_SIZE):
        client_index = randrange(0, len(client_id))
        jsonRegister.append(
            {
            "client_id": client_id[client_index],
            "client_name": client_name[client_index],
            "user_ID": getIdCode(),
            "name": names.get_full_name(),               
            "channel": random.choice(channel),
            "symptoms": getSymptoms(symptoms),                         
            "services": random.choice(services),
            "questions": [
                {"Teve_contato_com_alguem_de_covid": random.choice(boolean)}, 
                {"Quantos_dias_suspeitando_covid": randrange(1,6)}       
            ],
            "diagnostic" : random.choice(diagnostic), 
            "atendimento_humano": random.choice(boolean),  
            "nota_usuario" : randrange(1,10),
            "created_at" : dateList[i]                  
            }
        )

    myCollection.insert_many(jsonRegister)
    print(f"Populated into mongoDB with {SAMPLE_SIZE} documents.")

#streaming data insertion near real time
print("Streaming Data...")
while True:
    streamingRegister = []
    for i in range(0,randrange(1,STREAMING_SAMPLE_SIZE_RANGE)):
        client_index = randrange(0, len(client_id))
        streamingRegister.append(
                {
                "client_id": client_id[client_index],
                "client_name": client_name[client_index],
                "user_ID": getIdCode(),
                "name": names.get_full_name(),               
                "channel": random.choice(channel),
                "symptoms": getSymptoms(symptoms),                         
                "services": random.choice(services),
                "questions": [
                    {"Teve_contato_com_alguem_de_covid": random.choice(boolean)}, 
                    {"Quantos_dias_suspeitando_covid": randrange(1,6)}       
                ],
                "diagnostic" : random.choice(diagnostic),
                "atendimento_humano": random.choice(boolean),              
                "nota_usuario" : randrange(1,10),
                "created_at" : datetime.now()                  
                }
            ) 
    myCollection.insert_many(streamingRegister)
    time.sleep(10)



# %%
