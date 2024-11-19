import pandas as pd
import xml.etree.ElementTree as ET
import io
import uuid
from datetime import datetime

def infer_dtype(series):
    try:
        # Try to convert to numeric
        pd.to_numeric(series)
        # If series is balnk
        if set(series) == set(['']):
            return 'text'
        return 'number'
    except ValueError:
        try:
            # Try to convert to datetime
            pd.to_datetime(series)
            return 'datetime'
        except ValueError:
            # If both conversions fail, assume it's text
            return 'text'

def csvRowToBluePrismData(csv_string_with_headers):
    # Read the CSV string into a pandas DataFrame
    df = pd.read_csv(io.StringIO(csv_string_with_headers)).fillna('')
    dfDType = pd.DataFrame(columns=['column','dtype'])
    dfDType['column'] =df.columns
    for column in df.columns:
        dfDType.loc[dfDType['column']==column,'dtype'] = infer_dtype(df[column])

    # Create the root element for the XML
    root = ET.Element("collection")

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        row_element = ET.SubElement(root, "row")
        for header in df.columns:
            field_element = ET.SubElement(row_element, "field")
            field_element.set("name", header)
            field_element.set("type", dfDType.loc[dfDType['column']==header,'dtype'].values[0])
            field_element.set("value", str(row[header]))

    # Generate the XML string
    xml_string = ET.tostring(root, encoding="unicode")

    return xml_string

def createWorkQueueItemValues(csvString,queueId,queueIdent,keyColumn,priority,maxIdent):
    uniqueId = str(uuid.uuid4())
    currentDateTime = datetime.now().strftime(r'%Y-%m-%d %H:%M:%S.000')
    csvLines = csvString.split("\n")
    headerLine = csvLines[0]
    dataLines = csvLines[1:]
    if dataLines[-1] == '':
        dataLines.pop()
    valueArr = []
    for row in dataLines:
        csvSingle = '\n'.join([headerLine,row])
        data = csvRowToBluePrismData(csvSingle)
        dfData = pd.read_csv(io.StringIO(csvSingle))
        value = f"('{uniqueId}', '{queueId}', '{dfData[keyColumn].values[0]}','', 1, '{currentDateTime}', 0, '{data}',{queueIdent}, {maxIdent+1}, '{uniqueId}', {priority}, 0, NULL, NULL, NULL, 0, NULL, 0)"
        valueArr.append(value)
        maxIdent += 1
    values = ','.join(valueArr)
    return values
