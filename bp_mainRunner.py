import json
import os
import time
from bp_mainFunctions import *
import pandas as pd

CURR_PATH = os.path.abspath(os.path.dirname(__file__))
RUNNERCONFIG_PATH = os.path.join(CURR_PATH,'bp_runners.config')
with open(RUNNERCONFIG_PATH, 'r') as f:
        config = json.load(f)['mainRunner']

COMMANDFILE_PATH = os.path.join(config['shareDataFolderPath'],'bp_orchestratorCommands.csv')
WAIT_TIME = config['waitTime']

def __main__():
    while True:
        commands = pd.read_csv(COMMANDFILE_PATH,sep='|')
        print(commands)
        newCommands = commands[commands['status']=="Requested"]
        print(f"{len(newCommands)} new commands found")
        for i,c in newCommands.iterrows():
            commandPayload = json.loads(c['payload'])
            match c['command']:
                case "requestProcessStart":
                    print('Requesting a process start...')
                    requestProcessStart(commandPayload['scheduleName'])
                case "addToQueue":
                    print('Adding data to queue...')
                    addToQueue(commandPayload['scheduleName'],commandPayload['csvData'],commandPayload['priority'])
                case "startNotifications":
                    print('Starting Notification Runner...')
                    startNotifications()
                case "stopNotifications":
                    print('Stopping Notification Runner...')
                    stopNotifications()
                case _:
                    print(f"Error: command '{c['command']}' was not recognized")
                    commands.loc[i,'status'] = "Error"
                    continue
            commands.loc[i,'status'] = "Processed"
        commands.to_csv(COMMANDFILE_PATH,index=False)
        print(f'Sleeping for {WAIT_TIME} seconds...')
        time.sleep(WAIT_TIME)

__main__()
