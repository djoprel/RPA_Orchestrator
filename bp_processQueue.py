import pandas as pd
from datetime import datetime
import csv
import os
import json

CURR_PATH = os.path.abspath(os.path.dirname(__file__))
RUNNERCONFIG_PATH = os.path.join(CURR_PATH,'bp_runners.config')
with open(RUNNERCONFIG_PATH,'r') as f:
    config = json.load(f)
DATASHARE_FOLDER = config['mainRunner']['shareDataFolderPath']
QUEUE_CSV = os.path.join(DATASHARE_FOLDER,'bp_processQueue.csv')
ARCHIVE_CSV = os.path.join(DATASHARE_FOLDER,'bp_processQueueArchive.csv')

def getNewId():
    files = [QUEUE_CSV,ARCHIVE_CSV]
    latestId = 1
    for f in files:
        df = pd.read_csv(f)
        if len(df)>0:
            latestId = df['requestId'].max() + 1
            break
    
    return latestId

def pushToQueue(scheduleName):
    #Target file must contain a line break
    newId = getNewId()
    currentDateTime = datetime.now().strftime(r'%Y-%m-%d %H:%M:%S')
    with open(QUEUE_CSV,'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([newId,scheduleName,currentDateTime,''])
        

def markTriggered(queueId):
    currentDateTime = datetime.now().strftime(r'%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(QUEUE_CSV,index_col='requestId')
    df.loc[queueId,'triggerTime'] = currentDateTime
    df.to_csv(QUEUE_CSV,index='requestId')

def archiveTriggered():
    df = pd.read_csv(QUEUE_CSV)
    q_df = df[df['triggerTime'].isna()]

    a_df = pd.read_csv(ARCHIVE_CSV)
    archive_df = df[df['triggerTime'].isna() == False]
    a_df = pd.concat([a_df,archive_df])
    
    a_df.to_csv(ARCHIVE_CSV,index=False)
    q_df.to_csv(QUEUE_CSV,index=False)