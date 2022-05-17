## Parse UCB and ProConnect XML here
## The only in is the xml file
## The only out is the dataframe

from numpy import NaN
import requests
from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as E
import datetime
import pandas as pd
import time


twx_base_url = 'https://thingworx.steris.com' #Thingworx Production Environment
twx_app_key = '2aa02ebb-2ef7-444d-871b-8e0b2bb12d56' #Thingworx Production Environment

def list_all_accounts():
    url = twx_base_url + '/Thingworx/Things/ConnectQuestra/Services/ListAllDevices?appKey=' + twx_app_key
    body = {
    }
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'    
    }
    response = requests.post(url,json=body,headers=headers)
    accounts = response.json()['rows']
    assert response.status_code == 200
    return accounts

def list_all_alarms(ucb_account_id):
    url = twx_base_url + '/Thingworx/Things/ConnectQuestra/Services/ListServiceVisitLatestAlarmInfo?appKey=' + twx_app_key
    body = {
    'CustomerDeviceId': ucb_account_id
    }
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'    
    }
    response = requests.post(url,json=body,headers=headers)
    alarms = response.json()
    assert response.status_code == 200
    return alarms

def list_cycle_files(serial_number, start_ts, end_ts, ts_offset=240):
    '''List the cycle files in Thingworx
       Read the downloadLink property in the result for the download link
    '''    
    
    url = twx_base_url + '/Thingworx/Things/' + serial_number + '/Services/BaseFilterMyCsiqFiles?appKey=' + twx_app_key
    
    # Offset: time offset from GMT
    # Start Date: epoch time for start (in local time)
    # End Date: epoch time for end (in local time)
    
    # To get epoch time, go to https://www.epochconverter.com/, be sure to multiply by 1000 to include milliseconds
    body = {
        'DateTimeOffset': ts_offset,
        'StartDate': start_ts,  # epoch time with milliseconds
        'EndDate': end_ts
    }
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
    }
    
    # Download list of cycles
    response = requests.post(url,json=body,headers=headers)
    
    if response.status_code == 200:
        return response.json()['rows']


def read_cycle_file_as_xml(download_link):
    # Note: the endpoint returns relative links from Thignworx tight now but will return Azure endpoints with app code in next release
    is_full_link = download_link.startswith('http://') or download_link.startswith('https://')
    url = download_link if is_full_link else (twx_base_url + download_link)
    if '?' in url:
        url += "&appKey=" + twx_app_key
    else:
        url += "?appKey=" + twx_app_key    
        
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/octet-stream',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
    }
    
    if is_full_link:
        # Full endpoint already unzips
        response = requests.get(url,headers=headers)
        assert response.status_code == 200
        return response.content.decode('utf-8')
    else:        
        # Need to manually unzip
        response = requests.get(url,headers=headers)
        #print(response.status_code)
        #print(url)
        assert response.status_code == 200
        
        with BytesIO(response.content) as bf:
            with ZipFile(bf) as zf:
                for name in zf.namelist():
                    contents = zf.read(name).decode('utf-8')
                    return contents


def cycle_machine_details(file):
    header = ['AnalogDefinition','DigitalDefinition','PulseOutputDefinition','PulseInputDefinition']
    details = {}
    r = E.fromstring(file)
    for x in r:
        if x.tag == 'Header':
            details['header'] = {}
            for q in x:
                if q.tag not in header:
                    if q.text != None:
                        if '\n' in q.text:
                            details['header'][q.tag] = {}
                            for t in q:
                                print(q.tag,t.tag,q.text)
                                details['header'][q.tag][t.tag] = t.text
                        details['header'][q.tag] = q.text
                    else:
                        details['header'][q.tag] = q.text
    print(details)
            


def timeAnalog(file):                 ### Maybe make the Label in IO dictionary consistent so that we can call it out later on creating the health status
    # r = E.fromstring(path)
    r = E.fromstring(file)
    timeS = []                                  ## Initialization of time, sensor and state lists
    values = {}
    readingType = []

    abort = "* ABORT"
    i = 0
    tim = time.time()
    for x in r:
        if x.tag == "PrinterLines":
            for q in x:
                line = q.text
                if isinstance(line, str):
                    if abort in line:
                        cyclePass = False
                        break
                    else:
                        cyclePass = True
        elif x.tag == "AlarmWarnings":
            for q in x:
                if q.tag == "Alarms":
                    for t in q:
                        if t.tag == "Alarm":
                            cyclePass = False
        elif x.tag == "Header":
            if i == 0:
                for q in x:
                    if q.tag == "DeviceFamily":
                        machine = q.text
                    if q.tag == "AnalogDefinition":
                        for t in q:
                            for l in t:
                                if l.tag == "Label":
                                    values["%s"%l.text] = []
                    if q.tag == "DeviceFamily":
                        family = q.text
                    if q.tag == "DeviceModel":
                        model = q.text
        elif x.tag == "Analog":
            for qq in x:
                for t in qq:
                    if t.tag == "TimeStamp":
                        times = datetime.datetime.strptime(t.text, '%Y-%m-%dT%H:%M:%S')
                        timeS.append(times)
                    for l in t:
                        for st in l:
                            if st.tag == "Label":
                                label = st.text
                            if st.tag == "Value":
                                values["%s" % label].append(st.text)
    df = pd.DataFrame(index=timeS,columns=values.keys())
    for i in values:
        df[i] = values[i]
    if cyclePass == True:
        return df


def timeDigital(path): ## name would be a string of a cycle file, like ex. "030061707_20211213054847_8307_ar.xml"
    r = E.fromstring(path)
    df = pd.DataFrame()

    timeD = []                                  ## Initialization of time, sensor and state lists
    allLabels = []
    abort = "* ABORT"
    typename = NaN
    for x in r:
        if x.tag == "PrinterLines":
            for q in x:
                line = q.text
                if isinstance(line, str):
                    if abort in line:
                        cyclePass = False
                        break
                    else:
                        cyclePass = True
        elif x.tag == "AlarmWarnings":
            for q in x:
                if q.tag == "Alarms":
                    for t in q:
                        if t.tag == "Alarm":
                            cyclePass = False
        elif x.tag == "Header":
            for q in x:
                if q.tag == "DeviceFamily":
                    machine = q.text
                if q.tag == "DigitalDefinition":
                    for t in q:
                        for l in t:
                            if l.tag == "Label":
                                allLabels.append(l.text)
                if q.tag == "DeviceModel":
                    model = q.text
                if q.tag == "CycleName":
                    name = q.text
                if q.tag == "CycleTypeName":
                    typename = q.text
                if q.tag == "CycleCount":
                    count = q.text
        elif x.tag == 'Digital':
            for q in x:
                for t in q:
                    if t.tag=='TimeStamp':
                        # time = datetime.datetime.strftime(t.text, '%Y-%m-%dT%H:%M:%S')
                        times = datetime.datetime.strptime(t.text, '%Y-%m-%dT%H:%M:%S')          ## Parse xml file for date, sensor, and state
                        times.strftime('%Y-%m-%d %H:%M:%S')
                        if times not in timeD:
                            timeD.append(times)
            df = pd.DataFrame(index=timeD,columns=allLabels)
            for qq in x:
                for t in qq:
                    if t.tag=='TimeStamp':
                        times = datetime.datetime.strptime(t.text, '%Y-%m-%dT%H:%M:%S')
                        times.strftime('%Y-%m-%d %H:%M:%S')
                    for l in t:
                        for st in l:
                            if st.tag=='Label':
                                label = st.text
                            if st.tag=='Value':
                                df.loc[[times],[label]] = st.text
    if cyclePass == True:
        if df.empty == False:
            final = {
                "df": df,
                "machine": machine,
                "cyclePass": cyclePass,
                "model": model,
                "name": name,
                "typename": typename,
                "cyclecount": count
            }
            return final


def XParseAnalog(FolderPath):
    r = E.fromstring(FolderPath)

    timeA = []
    labelA = []
    valueA = []

    def isfl(string):
        try:
            float(string)
            return True
        except ValueError:
            return False
    def isnt(string):
        try:
            int(string)
            return True
        except ValueError:
            return False

    for x in r:
        if x.tag == "Header":
            for q in x:
                if q.tag == "DeviceFamily":
                    if "V-PRO" in q.text:
                        cycleType = "Vpro"
                if q.tag == "DeviceModel":
                    try:
                        q.text
                        if "AMSCO" in q.text or "Century" in q.text:
                            cycleType = "Amsco"
                        if "V-PRO" in q.text:
                            cycleType = "Vpro"
                    except:
                        pass
        if(x.tag == 'Analog'):
            for q in x:
                for t in q:
                    if(t.tag=='TimeStamp'):
                        time = t.text
                    for l in t:
                        for s in l:
                            st = s.text.replace(",",'')
                            if (isfl(st) == True):
                                valueA.append(float(st))
                            if(isfl(st) == False):
                                labelA.append(st)
                                timeA.append(time)
    time = []
    SE = {'Start':[],'End':[]}

    for x in r:
        if x.tag == "Events":
            for q in x[0:2]:
                for w in q:
                    if w.tag == 'TimeStamp':
                        time.append(w.text)

    SE['Start'] = time[0]
    SE['End'] = time[1]


    timeDt = pd.to_datetime(timeA)
    analog = {"Time":timeDt,"Label":labelA,"Value":valueA}
    Analog = pd.DataFrame(analog)
    # print(Analog.empty)           ## Shows if the analog is empty
    return [Analog,SE,cycleType]


def XParseDigital(FolderPath):
    r = E.fromstring(FolderPath)
    
    timeD = []
    labelD = []
    stateD = []
    
    def isfl(string):
        try:
            float(string)
            return True
        except ValueError:
            return False
    
    def isnt(string):
        try:
            int(string)
            return True
        except ValueError:
            return False


    for x in r:
        if(x.tag == 'Digital'):
            for q in x:
                for t in q:
                    if(t.tag=='TimeStamp'):
                        time = t.text
                    for l in t:
                        for st in l:
                            if (isnt(st.text) == True):
                                stateD.append(int(st.text))
                            if(isnt(st.text) == False):
                                labelD.append(st.text)
                                timeD.append(time)
                                
    timeDdt = pd.to_datetime(timeD)
    digital = {"Time":timeDdt,"Label":labelD,"State":stateD}    
    Digital = pd.DataFrame(digital)
    
    
    return Digital