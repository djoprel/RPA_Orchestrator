import time
import os
import json
import requests
import pandas as pd
from datetime import datetime
import csv
from bp_queries import getServerStates, getPastProcesses

CURR_PATH = os.path.abspath(os.path.dirname(__file__))

SERVERCONFIG_PATH = os.path.join(CURR_PATH,'bp_servers.config')
RUNNERCONFIG_PATH = os.path.join(CURR_PATH,'bp_runners.config')
with open(RUNNERCONFIG_PATH,'r') as f:
    config = json.load(f)
DATASHARE_FOLDER = config['mainRunner']['shareDataFolderPath']
NOTIFICATIONLOG_PATH = os.path.join(DATASHARE_FOLDER,'bp_notificationLogRecent.csv')
NOTIFICATIONARCHIVE_PATH = os.path.join(DATASHARE_FOLDER,'bp_notificationLogArchive.csv')

NOTIFICATIONTRIGGER_URL = config['notificationRunner']['triggerUrl']

def __main__():
    print('Notification Runner activated')
    runnerConfig = setStatus(True)['notificationRunner']
    with open(SERVERCONFIG_PATH,'r') as f:
        serverConfig = json.load(f)
    waitTime = runnerConfig['checkWaitSeconds']
    processNotStates = serverConfig['processNotificationStates']
    serverNotStates = serverConfig['serverNotificationStates']
    runtimeServers = serverConfig['runtimeResources']
    serverWaitTime = runnerConfig['serverConsistentStateWait']
    while True:
        processNotifications, perfectProcessCondition = checkProcessStatus(processNotStates)
        for i,pn in processNotifications.iterrows():
            sendNotification("Process",pn['processname'],pn['status'],pn['sessionid'])
        serverNotifications, perfectServerCondition = checkServerStatus(serverNotStates,runtimeServers,serverWaitTime)
        for i,sn in serverNotifications.iterrows():
            sendNotification("Server",sn['name'],sn['DisplayStatus'],"")
        if perfectProcessCondition and perfectServerCondition:
            print("Archiving")
            archiveNotifications()
        with open(RUNNERCONFIG_PATH,'r') as f:
            stopRequested = json.load(f)['notificationRunner']['stop']
        if stopRequested:
            setStop(False)
            setStatus(False)
            "Notification Runner stopping..."
            break
        print(f'Completed notification check, sleeping for {waitTime} seconds...')
        time.sleep(waitTime)

def setStatus(active):
    with open(RUNNERCONFIG_PATH,'r') as f:
        config = json.load(f)
    with open(RUNNERCONFIG_PATH,'w') as f:
        config['notificationRunner']['active'] = active
        json.dump(config,f,indent=4)
    return config

def setStop(stop):
    with open(RUNNERCONFIG_PATH,'r') as f:
        config = json.load(f)
    with open(RUNNERCONFIG_PATH,'w') as f:
        config['notificationRunner']['stop'] = stop
        json.dump(config,f,indent=4)

def checkProcessStatus(processNotStates):
    pastProcesses = getPastProcesses(hours=2)
    notificationProcesses = pastProcesses[pastProcesses['status'].isin(processNotStates)]
    perfectCondition = len(notificationProcesses)==0
    #Combo between sessionID and status hasn't been sent previously
    notificationProcesses['sessionstatus'] = notificationProcesses['sessionid'] + notificationProcesses['status']
    pastNotifications = pd.read_csv(NOTIFICATIONLOG_PATH)
    pastNotifications['sessionstatus'] = pastNotifications['sessionid'] + pastNotifications['status']
    newNotifications = notificationProcesses[~notificationProcesses['sessionstatus'].isin(pastNotifications['sessionstatus'].to_list())]
    return newNotifications, perfectCondition

def checkServerStatus(serverNotStates,runtimeServers,consistentStateWait):
    notificationServers = getNotServerStates(serverNotStates,runtimeServers)
    perfectCondition = len(notificationServers)==0
    if len(notificationServers)>0:
        print(f'Incorrect server state(s) identified, sleeping for {consistentStateWait} seconds to verify state consistency...')
        time.sleep(consistentStateWait)
        consistentServers = getNotServerStates(serverNotStates,runtimeServers)
        notificationServers = notificationServers[notificationServers.isin(consistentServers)].dropna()
    return notificationServers,perfectCondition

def getNotServerStates(serverNotStates,runtimeServers):
    serverStates = getServerStates()
    runtimeStates = serverStates[serverStates['name'].isin(runtimeServers)]
    notificationServers = runtimeStates[runtimeStates['DisplayStatus'].isin(serverNotStates)]
    return notificationServers

def sendNotification(type,origin,status,sessionid):
    currentDateTime = datetime.now().strftime(r'%Y-%m-%d %H:%M:%S')
    body = '{"type":"' + type + '","origin":"' + origin + '","status":"' + status + '"}'
    with open(NOTIFICATIONLOG_PATH,'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([type,origin,currentDateTime,status,sessionid])
    r = requests.post(NOTIFICATIONTRIGGER_URL,data=body,headers={'Content-Type':'application/json'})


def archiveNotifications():
    n_df = pd.read_csv(NOTIFICATIONLOG_PATH)
    a_df = pd.read_csv(NOTIFICATIONARCHIVE_PATH)
    a_df = pd.concat([a_df,n_df])

    a_df.to_csv(NOTIFICATIONARCHIVE_PATH,index=False)
    n_df[0:0].to_csv(NOTIFICATIONLOG_PATH,index=False)

#__main__()