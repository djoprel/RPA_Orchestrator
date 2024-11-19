import time
import os
import json
import pandas as pd
from bp_interactCommandLine import triggerProcess
from bp_queries import getScheduleServer,getServerStates
from bp_processQueue import markTriggered

CURR_PATH = os.path.abspath(os.path.dirname(__file__))


SERVERCONFIG_PATH = os.path.join(CURR_PATH,'bp_servers.config')
RUNNERCONFIG_PATH = os.path.join(CURR_PATH,'bp_runners.config')
with open(RUNNERCONFIG_PATH,'r') as f:
    config = json.load(f)
DATASHARE_FOLDER = config['mainRunner']['shareDataFolderPath']
QUEUE_PATH = os.path.join(DATASHARE_FOLDER,'bp_processQueue.csv')

def __main__():
    print('Process Queue Runner activated...')
    runnerConfig = setStatus(True)['queueRunner']
    with open(SERVERCONFIG_PATH,'r') as f:
        serverConfig = json.load(f)
    serverReadyStates = serverConfig['readyStates']
    waitTime = runnerConfig['statusCheckWaitSeconds']
    triggerWait = runnerConfig['postTriggerWaitSeconds']
    #while schedules left to run
    while True:
        #IMPROVEMENT: getNext per server
        nextSchedule = getNextInQueue()
        if len(nextSchedule) == 0:
            print('No schedules left')
            break
        scheduleName = nextSchedule['scheduleName'].values[0]
        #get schedule info
        scheduleServer = getScheduleServer(scheduleName)['resourcename'].values[0]
        #While server not ready
        while True:
            serverStates = getServerStates()
            serverStatus = serverStates[serverStates['name']==scheduleServer]['DisplayStatus'].values[0]
            if serverStatus in serverReadyStates:
                triggerProcess(scheduleName)
                markTriggered(nextSchedule['requestId'].values[0])
                time.sleep(triggerWait)
                break
            else:
                print(f'Server Status is "{serverStatus}", sleeping for {waitTime} seconds...')
                time.sleep(waitTime)
    setStatus(False)

def setStatus(active):
    with open(RUNNERCONFIG_PATH,'r') as f:
        config = json.load(f)
    with open(RUNNERCONFIG_PATH,'w') as f:
        config['queueRunner']['active'] = active
        json.dump(config,f,indent=4)
    return config

def getNextInQueue():
    df = pd.read_csv(QUEUE_PATH)
    df = df[df['triggerTime'].isna()]
    lowestId = df['requestId'].min()
    return df.loc[df['requestId']==lowestId]

__main__()