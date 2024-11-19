from bp_interactDatabase import executeQuery

def getWorkQueues():
    results = executeQuery("SELECT * FROM dbo.BPAWorkQueue",True)
    return results

def getSchedules():
    results = executeQuery("SELECT * FROM dbo.BPASchedule",True)
    return results

def getScheduleServer(scheduleName):
    results = executeQuery(f"SELECT resourcename FROM BPATaskSession WHERE taskid = (SELECT TOP 1 id FROM BPATask WHERE scheduleid = (SELECT id FROM BPASchedule WHERE name = '{scheduleName}'));",True)
    return results

def getWorkQueueItems():
    results = executeQuery("SELECT * FROM dbo.BPAWorkQueueItem",True)
    return results

def getServerStates():
    results = executeQuery("SELECT name,DisplayStatus FROM dbo.BPAResource",True)
    return results

def getPastProcesses(hours):
    results = executeQuery(f"""SELECT FilteredBPASession.sessionid, FilteredBPASession.startdatetime, BPAStatus.description AS status, BPAProcess.name AS processname, BPAResource.name AS resourcename
FROM (
    SELECT sessionid, startdatetime, processid, statusid, runningresourceid
    FROM BPASession
    WHERE startdatetime > DATEADD(hour, -{hours}, GETDATE())
) AS FilteredBPASession
INNER JOIN BPAStatus ON FilteredBPASession.statusid = BPAStatus.statusid
INNER JOIN BPAProcess ON FilteredBPASession.processid = BPAProcess.processid
INNER JOIN BPAResource ON FilteredBPASession.runningresourceid = BPAResource.resourceid;
""",True)
    return results

def insertWorkQueueItems(valuesString):
    #get max item ident
    query = f"""
                SET IDENTITY_INSERT [BluePrism].[dbo].[BPAWorkQueueItem] ON;

                INSERT INTO [BluePrism].[dbo].[BPAWorkQueueItem] (
                    id, queueid, keyvalue, 
                    status, attempt, loaded, worktime, 
                    data, 
                    queueident, ident, sessionid, priority, 
                    prevworktime, encryptid, locktime, lockid, sla, sladatetime, issuggested
                )
                VALUES 
                {valuesString};

                SET IDENTITY_INSERT [BluePrism].[dbo].[BPAWorkQueueItem] OFF;
            """
    executeQuery(query,False)
