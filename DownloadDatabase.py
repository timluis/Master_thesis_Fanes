import pandas as pd
import json
import os
import glob
import sqlite3
from sqlalchemy import create_engine
import pickle
import sys
import requests
import datetime
import numpy as np
import time
import itertools
from tqdm.notebook import tqdm
import Processing as pc
from dateutil.relativedelta import relativedelta


def UpdateAll():
    """ Downloads data, updates raw database and processed database for all station in list stations"""

    stations = ['Rossalm','Wolkenstein','Piz La Ila','St. Martin','St. Veit','Piz Pisciadu']
    for s in tqdm(stations):
        print('Collecting data for '+ s)
        test=GetData(s)
        if test == False:
            continue
        print('Adding data from '+s+' to raw database')
        UpdateDatabase(s)
        print('Processing data from '+s)
        UpdateProcessed(s)
        print('Finished station '+s)
        print('')


def RemoveFilefromFilelist(station,file_name):
    """ Remove files from filelist
    station: string
    file_name: string"""
    filelist_path = 'WeatherData/Filelists'
    try:
        with open(os.path.join(filelist_path,station + 'Filelist.txt'),'rb') as fp:
            ls_old = pickle.load(fp)
            ls_new = [ x for x in ls_old if file_name not in x]
            if ls_old != ls_new:
                with open(os.path.join(filelist_path,station + 'Filelist.txt'),'wb') as fp:
                    pickle.dump(ls_new,fp)
                    print('Removed ' + file_name + ' from filelist')
            if ls_old == ls_new:
                print('File does not exist in filelist')
    except Exception as e:
        print(e)


def CheckForNewJsonData(station):
    """ Check if therer are new json files in the station folder
    station: string"""

    newfile=list()
    exclude= set(['Processed','1998_to_2014'])
    path_to_watch = 'WeatherData'
    filelist_path = 'WeatherData/Filelists'

    try:
        with open(os.path.join(filelist_path,station + 'Filelist.txt'),'rb') as fp:
            ls_old=pickle.load(fp)
            ls_new=list()
            for root, dirs, files in os.walk(path_to_watch):
                dirs[:]=[d for d in dirs if d not in exclude]
                for file in files:
                    if station in file:
                        if file.endswith('.json'):
                            list.append(ls_new,os.path.join(root, file))
        if ls_old == ls_new:
            print('There is no new raw data in directory:',path_to_watch)

        else:
            added= [f for f in ls_new if not f in ls_old]
            removed= [f for f in ls_old if not f in ls_new]
            if removed:
                ls_new=ls_new+removed
                with open(os.path.join(filelist_path,station +'Filelist.txt'),'wb') as fp:
                    pickle.dump(ls_new,fp)
                    print('missing file:',removed)
                    return removed
            if added:
                newfile=list(i for i in added)
                with open(os.path.join(filelist_path,station +'Filelist.txt'),'wb') as fp:
                    pickle.dump(ls_new,fp)
                    print('new file',added)
                return newfile
    except EOFError:
        l=list()
        with open(os.path.join(filelist_path,station +'Filelist.txt'),'wb') as fp:
            pickle.dump(l,fp)
        print('Could not find '+ station + 'FileList...created empty '+ station + 'FileList in directory:',filelist_path)

    except FileNotFoundError:
        l=list()
        with open(os.path.join(filelist_path,station + 'Filelist.txt'),'wb') as fp:
            pickle.dump(l,fp)
        for root, dirs, files in os.walk(path_to_watch):
                dirs[:]=[d for d in dirs if d not in exclude]
                for file in files:
                    if station in file:
                        if file.endswith('.json'):
                            list.append(newfile,os.path.join(root, file))
        with open(os.path.join(filelist_path,station +'Filelist.txt'),'wb') as fp:
            pickle.dump(newfile,fp)
        print('Could not find '+ station + 'FileList...created empty '+ station + 'FileList in directory:',filelist_path)
        print('Added all existing files in directory',path_to_watch)
        print('Existing files:',newfile)
        return newfile


def LoadNewJsonData(station):
    """ Load all new json data into dataframes and combine them
    station: string"""
    df_LT=pd.DataFrame()
    df_LF=pd.DataFrame()
    df_HS=pd.DataFrame()
    df_N=pd.DataFrame()
    df_SD=pd.DataFrame()
    df_GS=pd.DataFrame()
    try:
        newfile=CheckForNewJsonData(station)
        print()
        for f in newfile:
            if 'LT' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_LT=df_LT.append(df,ignore_index=True)
            if 'LF' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_LF=df_LF.append(df,ignore_index=True)
            if 'HS' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_HS=df_HS.append(df,ignore_index=True)
            if 'N' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_N=df_N.append(df,ignore_index=True)
            if 'GS' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_GS=df_GS.append(df,ignore_index=True)
            if 'SD' in f:
                print('Reading file '+ os.path.split(f)[1] + ' to dataframe...')
                df=pd.read_json(f)
                df_SD=df_SD.append(df,ignore_index=True)
        print('Rename columns...')
        df_LT=df_LT.rename(columns={'DATE':'Datum','VALUE':'LT'})
        df_LF=df_LF.rename(columns={'DATE':'Datum','VALUE':'LF'})
        df_HS=df_HS.rename(columns={'DATE':'Datum','VALUE':'HS'})
        df_N=df_N.rename(columns={'DATE':'Datum','VALUE':'N'})
        df_GS=df_GS.rename(columns={'DATE':'Datum','VALUE':'GS'})
        df_SD=df_SD.rename(columns={'DATE':'Datum','VALUE':'SD'})

        print('Check for empty dataframes...')
        if df_N.empty == True:
            df_N['Datum'] = "NaN"
            df_N['N'] = "NaN"
        if df_LT.empty == True:
            df_LT['Datum'] = "NaN"
            df_LT['LT'] = "NaN"
        if df_LF.empty == True:
            df_LF['Datum'] = "NaN"
            df_LF['LF'] = "NaN"
        if df_HS.empty == True:
            df_HS['Datum'] = "NaN"
            df_HS['HS'] = "NaN"
        if df_SD.empty == True:
            df_SD['Datum'] = "NaN"
            df_SD['SD'] = "NaN"
        if df_GS.empty == True:
            df_GS['Datum'] = "NaN"
            df_GS['GS'] = "NaN"

        len_ls = []
        if df_LT.empty == False:
            df_LT = df_LT.sort_values(by='Datum')
            empty_df = pd.date_range(start=df_LT.loc[df_LT.index[0],'Datum'],end=df_LT.loc[df_LT.index[-1],'Datum'],freq='5T').to_frame(index=False,name='Datum')
        if df_LT.empty == True and df_SD.empty == False:
            df_SD = df_SD.sort_values(by='Datum')
            empty_df = pd.date_range(start=df_SD.loc[df_SD.index[0],'Datum'],end=df_SD.loc[df_SD.index[-1],'Datum'],freq='5T').to_frame(index=False,name='Datum')

        print('Join all dataframes into one...')
        df_new = empty_df.set_index('Datum').join(df_LT.set_index('Datum')).join(df_LF.set_index('Datum')).join(df_HS.set_index('Datum')).join(df_N.set_index('Datum')).join(df_GS.set_index('Datum')).join(df_SD.set_index('Datum'))
        df_new = df_new.dropna(how='all')

        print('Adding station ID...')
        station_ID = GetStationID(station)
        df_new['ID']= station_ID
        df_new.insert(0,'ID_2',range(0,len(df_new)))
        df_new = df_new.reset_index().set_index('ID_2')

        return df_new
    except Exception as e:
        print(e)
        print('Nothing could be add to the database')
        return pd.DataFrame()

def CheckDatabaseEntrys(df_new,station):
    """ Check if a dataframe is already in the database
    df_new: dataframe
    station: string"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    date_list = df_new['Datum'].tolist()
    date1 = date_list[0]
    date2 = date_list[-1]
    date1=date1.strftime('%Y-%m-%d %H:%M:%S.%f')
    date2=date2.strftime('%Y-%m-%d %H:%M:%S.%f')
    data = tuple()
    station_ID = GetStationID(station)
    try:
        for date in (date1,date2):
            cur.execute("SELECT ID_2 FROM Data WHERE Datum = ? AND ID=?", (date,station_ID))
            data1=cur.fetchone()
            print('In Line',data1,'existing date found' )
            if data1 != None:
                data = data + data1
        return data
    except TypeError as e:
        print(e)
        print('Database empty...')
        return data

def ReadDataFromSQLbyDate(station,datefrom,dateto):
    """ Reads from raw database
    station: string
    datefrom: string (format: YYYYMMDD)
    dateto: string (format: YYYYMMDD)"""
    engine=create_engine('sqlite:///Database/WeatherDatabase.db')
    station_ID = GetStationID(station)
    sql="""
    SELECT * FROM Data
    WHERE ID = :station_ID
    AND Datum BETWEEN :datefrom AND :dateto
    """

    df_old = pd.read_sql(sql,con=engine,index_col='ID_2',params={'station_ID':station_ID,'datefrom':datefrom,'dateto':dateto})
    return df_old

def ReadDataFromSQLbyID(station,ID_2_1,ID_2_2):
    """ Reads from raw database by ID
    station: string
    ID_2_1: int
    ID_2_2: int"""
    engine=create_engine('sqlite:///Database/WeatherDatabase.db')
    station_ID = GetStationID(station)
    sql="""
    SELECT * FROM Data
    WHERE ID_2 BETWEEN :ID_2_1 AND :ID_2_2
    AND ID = :station_ID
    """

    df_old = pd.read_sql(sql,con=engine,index_col='ID_2',params={'ID_2_1':ID_2_1,'ID_2_2':ID_2_2,'station_ID':station_ID})
    return df_old


def UpdateDatabase(station):
    """ Updates raw database if there is new json data
    station: string"""
    engine=create_engine('sqlite:///Database/WeatherDatabase.db')
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()

    print('Loading json data from directory')

    df_new=LoadNewJsonData(station)
   
    if df_new.empty == True:
        return

    print('...')
    print('Checking for existing data')
    print('...')
    data=CheckDatabaseEntrys(df_new,station)

    try:
        if data[0] and data[1]:
            print('There are already entries for this station at these dates')
            print('Data will be combined and added to database old data will be deleted')
            df_old=ReadDataFromSQLbyID(station,data[0],data[1])

            df_old.update(df_new)
            print('Dataframes combinded')

            try:
                ID_2_1=data[0]
                ID_2_2=data[1]
            except IndexError:
                print('One of the checked dates is not in the database...')
                print('Try to find it manually')
                sys.exit(1)

            station_ID = GetStationID(station)

            sql= "DELETE FROM Data WHERE ID_2 BETWEEN ? AND ? AND ID = ? "
            cur.execute(sql,(ID_2_1,ID_2_2,station_ID,))
            con.commit()
            print('Old rows deleted from database')

            df_old.to_sql('Data',con=engine,if_exists='append')
            print('New data added to database')
            con.commit()
            con.close()
        else:
            print('There are no existing entries.')
            print('Data will be added to database')

            df_new.to_sql('Data',con=engine,if_exists='append',index=False)
            print('Data succesfully added to database')
            con.commit()
            con.close()
    except IndexError:
        try:
            if data[0]:
                print('One of the dates already exists in the database')
                ID_sql = GetLatestIDFromRawData(station)
                try:
                    if data[0] == ID_sql:
                        print('Only first row are the same')
                        print('Row deleted')
                        df_new.drop(df_new.index[0])
                        df_new.to_sql('Data',con=engine,if_exists='append',index=False)
                        print('Data succesfully added to database')
                        con.commit()
                        con.close()
                    if data[0] != ID_sql:
                        print('I gave up ... retry with something else')
                except IndexError:
                    print('I gave up ... retry with something else')
                    raise
        except IndexError:
            print('There are no existing entries.')
            print('Data will be added to database')
            df_new.to_sql('Data',con=engine,if_exists='append',index=False)
            print('Data succesfully added to database')
            con.commit()
            con.close()



def CheckForDoubleRows():
    """ Check if there are duplicates in the rawdatabase"""
    engine=create_engine('sqlite:///Database/WeatherDatabase.db')
    df_double = pd.read_sql(sql="SELECT COUNT(*) FROM data GROUP BY ID_2 AND Datum HAVING COUNT(*)=2",con=engine)
    return df_double

def GetStationID(station):
    """ Get the station ID
    station: string"""
    if station == 'Piz La Ila':
        station_ID = 1
        return station_ID
    if station == 'Rossalm':
        station_ID = 2
        return station_ID
    if station == 'Piz Pisciadu':
        station_ID = 3
        return station_ID
    if station == 'Wolkenstein':
        station_ID = 4
        return station_ID
    if station == 'St. Martin':
        station_ID = 5
        return station_ID
    if station == 'St. Veit':
        station_ID = 6
        return station_ID

def GetStationNames():
    """ Get a list of station names"""
    stationlist = ['Piz La Ila','Rossalm','Piz Pisciadu','Wolkenstein','St. Martin','St. Veit']
    return stationlist

def GetData(station_name,sensor=None,datum1=None,datum2=None,dir_name='WeatherData'):
    """ Download data from the open data portal from South Tyrol
    station_name: string
    sensor: string
    datum1: string (format: YYYYMMDD)
    datum2: string (format: YYYYMMDD)
    dir_name: path 
    """

    if sensor == None:
        sensor = CheckSensorsFromRaw(station_name)
    sensor= sensor.split(',')
    station_ID = GetStationID(station_name)

    # check database entrys for station code
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    sql= """SELECT Station_code FROM Stations WHERE Station_name = ? """
    cur.execute(sql,(station_name,))
    station_sql = cur.fetchone()
    if station_sql:
        print('The station code is :' + station_sql[0])
        station = station_sql[0]
    else:
        print('Your station could not be found in the database')
        print('Please enter your station code: ')
        station = input('Enter Station code:')

    if datum1 == None:
        print('Looking for the latest date in database...')
        sql= """SELECT Datum FROM data WHERE ID=? ORDER BY Datum DESC LIMIT 1"""
        cur.execute(sql,(station_ID,))
        datum_sql = cur.fetchone()
        datum1 = datum_sql[0][:10].replace('-','')
        print('The latest date is: '+ datum1)

    if datum2 == None:
        today = datetime.date.today().strftime("%Y%m%d")
        datum2 = today
        print('Second date is set to present day...', datum2)
    con.commit()
    con.close()
    try:
        if datum1 == datum2:
            raise NoNewData
    except NameError:
        print('No new data to download')
        return False

    start_date = datetime.datetime.strptime('20140801',"%Y%m%d")
    test_date = datetime.datetime.strptime(datum1,"%Y%m%d")
    if test_date < start_date:
        print('Data can only be collected from 20140801 onwards your start date changed')
        datum1 = '20140801'

    try:
        os.makedirs(os.path.join(dir_name, station_name))
        print('Created folder ' + station_name)
    except FileExistsError:
        pass

    for s in sensor:
        date_lst=DivideIntoChunks(datum1,datum2,s)
        if date_lst:
            try:
                for r in range(len(date_lst)):
                    DownloadSensor(s,station_name,station,date_lst[r],date_lst[r+1],dir_name)
            except IndexError:
                pass
        else:
            DownloadSensor(s,station_name,station,datum1,datum2,dir_name)



def DownloadSensor(sensor,station_name,station,datum1,datum2,dir_name):
    """ Download a certain sensor from the open data portal and save as json file
    sensor: string
    station_name: string
    station: int
    datum1: string
    datum2: string
    dir_name: path"""

    dir_url = 'http://daten.buergernetz.bz.it/services/meteo/v1/timeseries?station_code='
    suffix ='.json'
    if sensor != '':
        if sensor != 'N':
            print('Collecting data for ...' + sensor)
            data = json.loads(requests.get(os.path.join(dir_url + station + '&sensor_code=' + sensor + '&date_from=' + datum1 + '&date_to=' + datum2)).text)
            with open(os.path.join(dir_name,station_name, station_name + '_' + sensor + '_from_' + datum1 + '_to_' + datum2 + suffix), 'w+') as outfile:
                json.dump(data,outfile)
        if sensor == 'N':
            print('Collecting data for ...' + sensor)
            data = json.loads(requests.get(os.path.join(dir_url + station + '&sensor_code=' + sensor + '&date_from=' + datum1 + '&date_to=' + datum2)).text)
            with open(os.path.join(dir_name,station_name, station_name + '_' + sensor + '_from_' + datum1 + '_to_' + datum2 + suffix), 'w+') as outfile:
                json.dump(data,outfile)

def DivideIntoChunks(datum1,datum2,sensor):
    """ Split data into chunks if size to big to download at once
    datum1: string
    datum2: string
    sensor: string"""

    date1_old = datetime.datetime.strptime(datum1,"%Y%m%d")
    date2_old = datetime.datetime.strptime(datum2,"%Y%m%d")
    delta = date2_old - date1_old
    print('Days between your dates :', delta.days)
    if sensor != 'N':
        num = 835
    if sensor == 'N':
        num = 418

    if delta.days >= num:
        print('Your timespan is to long to download in one file')
        div = int(np.ceil(delta.days/num))
        print('Your timespan will be splitted into ',div,'files')
        part_duration = delta.days / div
        days_lst = [(np.ceil((i + 1) * part_duration)) for i in range(div)]
        date_lst = [datum1]+[(datetime.datetime.strftime(date1_old + datetime.timedelta(days_lst[r]),"%Y%m%d")) for r in range(0,div)]
        print('New timespans set to: ')
        print(date_lst)
        return date_lst




def ReadfromRawData(station,sensor,startdate='',enddate=''):
    """ Read raw data from SQL database. Startdate and Enddate in YYYYMMDD format
    station: string
    sensor: string
    statdate: string
    enddate: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    station_ID = GetStationID(station)
    if len(enddate) == 8:
        enddate = datetime.datetime.strptime(enddate,'%Y%m%d')
        enddate = datetime.datetime.strftime(enddate,'%Y-%m-%d %H:%M:%S.%f')
    if enddate == '':
        enddate = GetLatestDateFromRawData(station)

    if len(startdate) == 8:
        startdate = datetime.datetime.strptime(startdate,'%Y%m%d')
        startdate = datetime.datetime.strftime(startdate,'%Y-%m-%d %H:%M:%S.%f')
    if startdate == '':
        startdate = GetOldestDateFromRawData(station)



    sql=("SELECT ID_2,ID,Datum,"+sensor+" FROM Data WHERE ID = ? AND Datum BETWEEN ? AND ?")
    cur.execute(sql,(station_ID,startdate,enddate,))
    result = cur.fetchall()
    column_names=['ID_2','ID','Datum']+[(sensor.split(',')[n])for  n in range(len(sensor.split(',')))]
    df = pd.DataFrame(result,columns=column_names)
    df['Datum'] = pd.to_datetime(df['Datum'])
    df = df.set_index('ID_2')
    return df

def UpdateProcessed(station):
    """ Update processed database
    station: string"""
    engine=create_engine('sqlite:///Database/WeatherDatabase.db')
    sensor = CheckSensorsFromRaw(station)
    rules = ['Hourly','Daily','Weekly','Monthly']
    for rule in rules:
        print('Process data to ' + rule + ' values')
        rule, rule_ac = Rules(rule)
        startdate = GetLatestDateFromProcessed(station,rule)
        enddate = GetLatestDateFromRawData(station)
        try:
            if rule == 'Hourly':
                enddate_datetime=datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S.%f')
                enddate = enddate_datetime - datetime.timedelta(minutes=1)
                enddate = str(enddate)
                print('Full data available till '+ enddate)
                if startdate[:10] == enddate[:10]:
                    raise ErrorSameDates
            if rule == 'Daily':
                #print('Process data Daily')
                enddate_datetime=datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S.%f')
                enddate = enddate_datetime - datetime.timedelta(days=1)
                enddate = datetime.datetime.combine(enddate,datetime.time(23,50))
                enddate = str(enddate)
                print('Full data available till ' + enddate)
                if startdate[:10] == enddate[:10]:
                    raise ErrorSameDates
            if rule == 'Weekly':
                #print('Process data Weekly')
                try:
                    #startdate_datetime=datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S.%f')
                    enddate_datetime=datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S.%f')
                    enddate = enddate_datetime - datetime.timedelta(days=enddate_datetime.isoweekday())
                    enddate = datetime.datetime.combine(enddate,datetime.time(23,50))
                    enddate = str(enddate)
                    print('Full data available till ' + enddate)
                    if startdate[:10]==enddate[:10]:
                        raise ErrorSameDates
                except TypeError:
                    pass
            if rule == 'Monthly':
                #print('Process data Monthly')
                enddate_datetime=datetime.datetime.strptime(enddate,'%Y-%m-%d %H:%M:%S.%f')
                lastDayofMonth= enddate_datetime.replace(day=1) - datetime.timedelta(days=1) + datetime.timedelta(hours=23,minutes=50)
                enddate = str(lastDayofMonth)
                print('Full data available till ' + enddate)
                try:
                    if startdate[:7] == enddate[:7]:
                        raise ErrorSameDates
                except TypeError:
                    pass

            if rule == 'movAVG' and 'HS' in sensor:
                sensor = 'HS,LT'
                df=ReadfromRawData(station,sensor,startdate,enddate)
                df=pc.Snow_processing(df)
                df=df.set_index('Datum')
                ps=df.HS.rolling(42,min_periods=30).mean()
                df=pd.DataFrame(ps)
                df['ID']=GetStationID(station)
                df=df.reset_index()
                df = df[['ID'] + [col for col in df.columns if col !='ID']]
                df.to_sql(rule,con=engine,if_exists='append',index=False)
                print('Processed data added to database')


            if 'N' not in sensor and rule != 'movAVG':
                df=ReadfromRawData(station,sensor,startdate,enddate)
                if 'HS' in sensor:
                    df=pc.Snow_processing(df)
                if 'LT' in sensor:
                    df=pc.LT_processing(df)
                if 'LF' in sensor:
                    df=pc.LF_processing(df)
                df_daily = df.set_index('Datum')
                df_daily = df_daily.resample(rule=rule_ac).mean()
                df_daily = df_daily.reset_index()
                df_daily.to_sql(rule,con=engine,if_exists='append',index=False)
                print('Processed data added to database')
                print('')

            if 'N' in sensor and rule != 'movAVG':
                df=ReadfromRawData(station,sensor,startdate,enddate)
                if 'HS' in sensor:
                    df=pc.Snow_processing(df)
                if 'LT' in sensor:
                    df=pc.LT_processing(df)
                if 'LF' in sensor:
                    df=pc.LF_processing(df)
                if 'GS' in sensor:
                    df=pc.GS_processing(df)
                if 'SD' in sensor:
                    df=pc.SD_processing(df)
                df=pc.N_processing(df)
                df_daily = df.set_index('Datum')
                if 'GS' in sensor or 'SD' in sensor:
                    df_daily = df_daily.resample(rule=rule_ac).agg({'LT': np.mean, 'N': np.sum, 'LF': np.mean, 'ID':np.mean, 'SD':np.sum, 'GS':np.sum})
                else:
                    df_daily = df_daily.resample(rule=rule_ac).agg({'LT': np.mean, 'N': np.sum, 'LF': np.mean, 'ID':np.mean})
                df_daily = df_daily.reset_index()
                df_daily.to_sql(rule,con=engine,if_exists='append',index=False)
                print('Processed data added to database')
                print('')
        except NameError:
            print('Data already processed no new raw data to process')
            print('')

def ReadfromProcessed(station,rule,sensor='',startdate='',enddate=''):
    """ Read from processed database
    station: string
    rule: string
    sensor: string
    statdate: string/datetime
    enddate: string/datetime"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    if sensor == '':
        sensor = CheckSensorsFromRaw(station)
    if len(enddate) == 8:
        enddate = datetime.datetime.strptime(enddate,'%Y%m%d')
        enddate = datetime.datetime.strftime(enddate,'%Y-%m-%d %H:%M:%S.%f')
    if len(startdate) == 8:
        startdate = datetime.datetime.strptime(startdate,'%Y%m%d')
        startdate = datetime.datetime.strftime(startdate,'%Y-%m-%d %H:%M:%S.%f')
    if startdate == '':
        startdate = GetOldestDateFromProcessed(station,rule)
    if enddate == '':
        enddate = GetLatestDateFromProcessed(station,rule)
    station_ID = GetStationID(station)
    rule, rule_ac = Rules(rule)
    if rule == 'movAVG':
        sensor='HS'
    sql=("SELECT Datum,ID,"+sensor+" FROM " + rule + " WHERE ID = ? AND Datum BETWEEN ? AND ?")
    cur.execute(sql,(station_ID,startdate,enddate))
    result = cur.fetchall()
    column_names=['Datum','ID']+[(sensor.split(',')[n])for  n in range(len(sensor.split(',')))]
    df = pd.DataFrame(result,columns=column_names)
    df['Datum'] = pd.to_datetime(df['Datum'])
    df=df.sort_values('Datum')
    #df = df.set_index('ID_2')
    return df

def GetLatestDateFromProcessed(station,rule):
    """ Check latest date in the processed database
    station: string
    rule: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    station_ID = GetStationID(station)
    rule,rule_ac = Rules(rule)
    try:
        print('Looking for the latest date in '+ rule)
        sql= "SELECT Datum FROM "+ rule +" WHERE ID=? ORDER BY Datum DESC LIMIT 1"
        cur.execute(sql,(station_ID,))
        datum_sql = cur.fetchone()
        con.close()
        print('The latest date in '+ rule + ' is ', datum_sql[0][:10])
        return datum_sql[0]
    except TypeError:
        print('No processed data took first date from raw database')
        startdate= GetOldestDateFromRawData(station)
        return startdate

def GetOldestDateFromProcessed(station,rule):
    """ Check oldest date in processed database:
    station: string
    rule: string"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    station_ID = GetStationID(station)
    rule,rule_ac = Rules(rule)
    try:
        print('Looking for the oldest date in '+ rule)
        sql= "SELECT Datum FROM "+ rule +" WHERE ID=? ORDER BY Datum ASC LIMIT 1"
        cur.execute(sql,(station_ID,))
        datum_sql = cur.fetchone()
        con.close()
        print('The oldest date in '+ rule + ' is ', datum_sql[0][:10])
        return datum_sql[0]
    except TypeError:
        print('No processed data took first date from raw database')
        startdate= GetOldestDateFromRawData(station)
        return startdate

def GetLatestDateFromRawData(station):
    """ Check latest date in raw data database:
    station: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    print('Looking for the latest date in raw database')
    station_ID = GetStationID(station)
    sql= """SELECT Datum FROM Data WHERE ID=? ORDER BY Datum DESC LIMIT 1"""
    cur.execute(sql,(station_ID,))
    datum_sql = cur.fetchone()
    #datum1 = datum_sql[0][:10].replace('-','')
    print('The latest date in raw database is: ', datum_sql[0])
    con.close()
    return datum_sql[0]

def GetLatestIDFromRawData(station):
    """ Check latest ID in processed database:
    station: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    print('Looking for the latest ID in database...')
    station_ID = GetStationID(station)
    sql= """SELECT ID_2 FROM Data WHERE ID=? ORDER BY Datum DESC LIMIT 1"""
    cur.execute(sql,(station_ID,))
    ID_sql = cur.fetchone()
    #datum1 = datum_sql[0][:10].replace('-','')
    print('The latest ID is: ', ID_sql[0])
    con.close()
    return ID_sql[0]

def GetOldestDateFromRawData(station):
    """ Check oldest date in raw data database:
    station: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    print('Looking for the oldest date in database...')
    station_ID = GetStationID(station)
    sql= """SELECT Datum FROM Data WHERE ID=? ORDER BY Datum ASC LIMIT 1"""
    cur.execute(sql,(station_ID,))
    datum_sql = cur.fetchone()
    #datum1 = datum_sql[0][:10].replace('-','')
    print('The oldest date is: ', datum_sql[0])
    con.close()
    return datum_sql[0]

def Rules(rule):
    """ Function to convert rules into abbreviation
    rule: string"""
    try:
        if rule == 'Hourly' or rule == 'h':
            rule = 'Hourly'
            rule_ac = 'h'
        if rule == 'Daily' or rule == 'd':
            rule='Daily'
            rule_ac='d'
        if rule == 'Monthly'or rule == 'm':
            rule = 'Monthly'
            rule_ac= 'm'
        if rule == 'Weekly' or rule == 'w':
            rule = 'Weekly'
            rule_ac = 'w'
        if rule == 'movAVG' or rule == 'Moving' or rule == 'mAVG':
            rule= 'movAVG'
            rule_ac= 'mAVG'
        return rule, rule_ac
    except UnboundLocalError:
        print('Your rule is not supported in this code please choose either Daily, Monthly or Weekly')
        rule= ''
        rule_ac=''
        return rule, rule_ac

def CheckSensorsFromRaw(station):
    """ Check which sensors are available for a certain station
    station: string"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    station_ID = GetStationID(station)
    sensor = 'LT,LF,HS,N,SD,GS'
    sql=("SELECT ID_2,ID,Datum,"+sensor+" FROM Data WHERE ID = ?  ORDER BY Datum DESC LIMIT 10")
    cur.execute(sql,(station_ID,))
    result = cur.fetchall()
    con.close()

    range1=range(3,9)
    range2=range(0,9)
    sensorNot = [(sensor.split(',')[y-3]) for y,x in itertools.product(range1,range2) if result[x][y] == None and result[x+1][y] == None]
    ls = [9*n for n in range(1,6)]
    for n in ls:
        if len(sensorNot)==n:
            try:
                sensorRemove=[sensorNot[0+j*9] for j in range(0,(len(sensorNot)//9))]
                sensorNew = sensor
                for k in range(0,len(sensorRemove)):
                    sensorNew = sensorNew.replace(','+sensorRemove[k],'')
                return sensorNew
            except IndexError:
                print('Index Error')
                pass

    "old way of checking sensors with mistake"
    """try:
        sensorNot = [(sensor.split(',')[y-3]) for y,x in itertools.product(range1,range2) if result[x][y] == None and result[x+1][y] == None]
        if len(sensorNot) == 9:
            if sensorNot[1:] == sensorNot[:-1]:
                sensorNew = sensor.replace(','+sensorNot[0],'')
        if len(sensorNot) == 18:
            sensor1 = sensorNot[:(len(sensorNot)//2)]
            sensor2 = sensorNot[(len(sensorNot)//2):]
            if sensor1[1:] == sensor1[:-1] and sensor2[1:] == sensor2[:-1]:
                sensorNew = sensor.replace(','+sensor1[0],'').replace(','+sensor2[0],'')
        return sensorNew
    except IndexError:
        pass"""

def CreateNewDatabse():
    try:
        con = sqlite3.connect('Database/WeatherDatabase.db')
        cur = con.cursor()
        sql_command = """ CREATE TABLE Stations (
                            ID INTEGER PRIMARY KEY,
                            Station_name CHAR,
                            Station_code CHAR
                            );"""
        cur.execute(sql_command)
        sql_command = """INSERT INTO Stations (ID, Station_name, Station_code) 
                            VALUES 
                            (1, 'Piz La Ila','61690SF'),
                            (2, 'Rossalm','42830SF'),
                            (3, 'Piz Pisciadu','61720WS'),
                            (4, 'Wolkenstein','73500MS'),
                            (5, 'St. Martin','62600MS'),
                            (6, 'St. Veit','42700MS');"""
        cur.execute(sql_command)
        con.commit()
        con.close()
        RebuildRawDatabase()
        RebuildProcessedDatabase()        
    except Exception as e:
        print(e)


def RebuildRawDatabase():
    con = sqlite3.conncet('Database/WeatherDatabase.db')
    cur = con.cursor()
    try:
        sql_command = "DROP TABLE Data"
        cur.execute(sql_command)
    except Exception as e:
        print(e)
    sql_command = """CREATE TABLE Data (
                        ID_2 INTEGER PRIMARY KEY,
                        ID INTEGER,
                        Datum DATETIME,
                        LT FLOAT,
                        LF FLOAT,
                        HS FLOAT,
                        N FLOAT,
                        GS FLOAT,
                        SD FLOAT,
                        FOREIGN KEY(ID)
                            REFERENCES Stations(ID)
                        );"""
    cur.execute(sql_command)
    con.commit()
    con.close()

def RebuildProcessedDatabase():
    """ In case processed database need to be rebuild"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    rulelist = ['Hourly','Daily','Weekly','Monthly']
    for r in rulelist:
        print('Drop table ' + r + ' if exist')
        try:
            sql_command = "DROP TABLE " + r
            cur.execute(sql_command)
            print('Create new table '+r)
            sql_command = """
            CREATE TABLE """+ r +""" (
            ID_3 INTEGER PRIMARY KEY,
            ID INTEGER,
            Datum DATETIME,
            LT FLOAT,
            LF FLOAT,
            HS FLOAT,
            N FLOAT,
            GS FLOAT,
            SD FLOAT);"""
            cur.execute(sql_command)
        except Exception as e:
            print(e)
            print('Create table '+r)
            sql_command = """
            CREATE TABLE """+ r +""" (
            ID_3 INTEGER PRIMARY KEY,
            ID INTEGER,
            Datum DATETIME,
            LT FLOAT,
            LF FLOAT,
            HS FLOAT,
            N FLOAT,
            GS FLOAT,
            SD FLOAT);"""
            cur.execute(sql_command)
    con.commit()
    con.close()

def BuildMovingAverageTable():
    """ If the moving average table needs to be rebuild"""
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    try:
        sql_command = "DROP TABLE movAVG"
        cur.execute(sql_command)
        print('Dropped table movAVG')
        ql_command="""CREATE TABLE movAVG (
        ID_3 INTEGER PRIMARY KEY,
        ID INTEGER,
        Datum DATETIME,
        HS FLOAT);"""
        cur.execute(sql_command)
        print('Created table movAVG')
        con.commit()
        con.close()
    except:
        sql_command="""CREATE TABLE movAVG (
        ID_3 INTEGER PRIMARY KEY,
        ID INTEGER,
        Datum DATETIME,
        HS FLOAT);"""
        cur.execute(sql_command)
        print('Created table movAVG')
        con.commit()
        con.close()


def DropDuplicatesFromProcessed(station,rule):
    """ Drop all duplicates from the processed database
    station: string
    rule: string
    """
    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    station_ID = GetStationID(station)
    sql="SELECT count(Datum),count(DISTINCT Datum) FROM "+rule+" WHERE ID = ?"
    cur.execute(sql,(station_ID,))
    duplicates= cur.fetchall()
    delete= duplicates[0][0] - duplicates[0][1]
    print('Found ' + str(delete) + ' duplicate rows')
    if delete !=0:
        sql="DELETE FROM " + rule + " WHERE ID=? AND rowid NOT IN (SELECT MIN(rowid) FROM " + rule + " WHERE ID=? GROUP BY Datum)"
        cur.execute(sql,(station_ID,station_ID))
        print('Rows deleted')
    con.commit()
    con.close()

def add98_14ToProcessed():
    """ Add data which can't be downloaded to the database"""
    engine = create_engine('sqlite:///Database/WeatherDatabase.db')
    dir_path = 'WeatherData/1998_to_2014'
    stations = GetStationNames()
    file_ls=[filename for dirpath,dirnames,filename in os.walk(dir_path)][0]
    file_ls = [f for f in file_ls if f.endswith('.csv')]
    for s in stations:
        data = [d for d in file_ls if s.replace(' ','') in d]
        for d in data:
            df=pd.read_csv(os.path.join(dir_path,d),header=12,decimal=',',delimiter='\t')
            df.iloc[:,2] = [x.replace(',','.') for x in df.iloc[:,2]]
            df.iloc[:,2] = [x.replace('---','NaN') for x in df.iloc[:,2]]
            df.iloc[:,2] = df.iloc[:,2].astype(float)
            df.loc[df.iloc[:,3]!='40 (Gut)',df.columns[2]]=None
            df=df.drop(columns=[df.columns[1],df.columns[3]])
            if 'HS' in d:
                df=df.rename(columns={df.columns[1]:'HS'})
            elif 'LT' in d:
                df=df.rename(columns={df.columns[1]:'LT'})
            else:
                df=df.rename(columns={df.columns[1]:'N'})
            df['ID']=GetStationID(s)
            df['Datum'] = pd.to_datetime(df['Datum'],format='%d/%m/%Y')
            df=df.drop(df[df.Datum >= datetime.date(2014,8,1)].index)

            if 'Month' in d:
                df['Datum'] = [df['Datum'][n] + relativedelta(day=+31) for n in range(0,len(df['Datum']))]
                df.to_sql('Monthly',con=engine,if_exists='append',index=False)
            if 'Day' in d:
                df.to_sql('Daily',con=engine,if_exists='append',index=False)
                ProcessCSVDataDailyToMonthly(s)

def ProcessCSVDataDailyToMonthly(station):
    """ Process CSV data from daily values to monthly values
    station: string"""
    df_new=ReadfromProcessed(station,'Daily',enddate='20140731')
    df_new=df_new.set_index('Datum')
    df_new=df_new.drop(columns={'ID'})
    df_new=df_new.resample(rule='m').mean()
    df_new=df_new.dropna()
    df_old=ReadfromProcessed(station,'Monthly',enddate='20140731')
    df_old=df_old.set_index('Datum')
    df_old.update(df_new)
    df_old=df_old.reset_index()


    con = sqlite3.connect('Database/WeatherDatabase.db')
    cur = con.cursor()
    enddate='20140731'
    if len(enddate) == 8:
        enddate = datetime.datetime.strptime(enddate,'%Y%m%d')
        enddate = datetime.datetime.strftime(enddate,'%Y-%m-%d %H:%M:%S.%f')

    startdate = GetOldestDateFromProcessed(station,'Monthly')

    station_ID = GetStationID(station)

    sql= "DELETE FROM Monthly WHERE ID=? AND Datum BETWEEN ? AND ?"
    cur.execute(sql,(station_ID,startdate,enddate,))
    con.commit()
    con.close()
    print('Old rows deleted from database')

    engine = create_engine('sqlite:///Database/WeatherDatabase.db')

    df_old.to_sql('Monthly',con=engine,if_exists='append',index=False)
    print('New Data Frame added to database')

    return

def metadata_stations():
    """ Check the metadata from all stations at the open data portal """
    dir_url = 'http://daten.buergernetz.bz.it/services/meteo/v1/stations'

    data = json.loads(requests.get(dir_url).text)

    return data