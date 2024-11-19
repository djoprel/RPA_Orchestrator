import pyodbc
import json
import os
import pandas as pd

CURR_PATH = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(CURR_PATH,'bp_DB.config'), 'r') as f:
    config = json.load(f)

SERVER= config['SERVER']
DATABASE = config['DATABASE']

def __openConnection():
        # Establish a connection
        conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes")
        # Create a cursor
        cursor = conn.cursor()
        return conn,cursor


def __closeConnection(conn,cursor):
    cursor.close()
    conn.close()

def __queryDatabase(cursor,sqlStatement):
    # Execute SQL query
    cursor.execute(sqlStatement)

    # Fetch results
    results = cursor.fetchall()
    #for row in results:
        #print(row)  # Process the data as needed
    return results

def executeQuery(query, returnFormatted):
    try:
        conn,cursor = __openConnection()
        results = __queryDatabase(cursor,query)
        
        if returnFormatted:
            data = pd.DataFrame.from_records(results, columns=[col[0] for col in cursor.description])
        else:
            data = results
        __closeConnection(conn,cursor)
        return data
    except pyodbc.Error as e:
            print(f"Error: {e}")



