import json
import os
from bp_queries import *
from bp_formatQueueData import createWorkQueueItemValues
from bp_processQueue import pushToQueue
from bp_notificationRunner import setStop

CURR_PATH = os.path.abspath(os.path.dirname(__file__))
RUNNERCONFIG_PATH = os.path.join(CURR_PATH,'bp_runners.config')

def addToQueue(queueName,csvData,priority):
    df = getWorkQueues()
    queue = df.loc[df['name']==queueName]
    maxIdent = getWorkQueueItems()['ident'].max()
    sqlValues = createWorkQueueItemValues(csvData,queue['id'].values[0],queue['ident'].values[0],queue['keyfield'].values[0],priority,maxIdent)
    print(sqlValues)
    insertWorkQueueItems(sqlValues)

def requestProcessStart(scheduleName):
    pushToQueue(scheduleName)

    with open(RUNNERCONFIG_PATH, 'r') as f:
        config = json.load(f)['queueRunner']
    if not config['active']:
        os.startfile('python',arguments=os.path.join(CURR_PATH, 'bp_queueRunner.py'))
    else:
        print("Process Queue Runner already active...")

def startNotifications():
    with open(RUNNERCONFIG_PATH, 'r') as f:
        config = json.load(f)['notificationRunner']
    if not config['active']:
        os.startfile('python',arguments=os.path.join(CURR_PATH, 'bp_notificationRunner.py'))
    else:
        print("Notification Runner already active...")

def stopNotifications():
    setStop(True)