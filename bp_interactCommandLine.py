import subprocess
import json
import os
import pandas as pd

CURR_PATH = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(CURR_PATH,'bp_CLI.config'), 'r') as f:
    config = json.load(f)

EXE_LOCATION = config['EXE_LOCATION']
USERNAME = config['USERNAME']
PASSWORD = config['PASSWORD']

#CLI Documentation
#https://bpdocs.blueprism.com/bp-7-1/en-us/helpCommandLine.htm

def triggerProcess(scheduleName):
    #Start process via automateC.exe (automateC.exe /schedule "Schedule Name" /startschedule /user <un> <pwd>)
    subprocess.run(f'"{EXE_LOCATION}\\automateC.exe" /schedule "{scheduleName}" /startschedule /user {USERNAME} {PASSWORD}')
    
def getRanProcesses():
    #Get process log via automateC.exe (automateC.exe /resourcestatus all 1 d /user <un> <pwd>)
    #Time formats: d = day | h = hour
    processLog = subprocess.run(f'"{EXE_LOCATION}\\automateC.exe" /resourcestatus all 1 h /user {USERNAME} {PASSWORD}',capture_output=True,text=True)
    pastProcesses = reformatRanProcesses(processLog.stdout)
    return pastProcesses

def reformatRanProcesses(processStatuses):
    pastProcesses = processStatuses[processStatuses.find("Resource Name"):]
    processLines = pastProcesses.splitlines()
    output = []
    for l in processLines:
        line = []
        cols = l.split("  ")
        for c in cols:
            if c != "":
                line.append(c)
        output.append(line)
    dfOutput = pd.DataFrame(data=output[1:],columns=output[0])
    return dfOutput