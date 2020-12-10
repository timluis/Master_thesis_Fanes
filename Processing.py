import pandas as pd
import sqlite3
import datetime
import DownloadDatabase as dd
import numpy as np 
import math
import os


def Snow_processing(df):
    """ Clear bad snow measurments from raw data """
    print('Tidying snow data')
    df.loc[(df['HS']<0),'HS']= None
    df.loc[ df['HS']>250,'HS'] = None
    df.loc[(df['Datum'].dt.month >= 7) & (df['Datum'].dt.month  <= 9) & (df['HS']>5), 'HS'] = 0
    #df.loc[(df['Datum'].dt.month >= 6) & (df['Datum'].dt.month  <= 10) & (df['HS']>20), 'HS'] = 0
    df.loc[((df['HS']-df['HS'].shift())>10),'HS']=None
    df.loc[((df['HS']-df['HS'].shift())<-10),'HS']=None
    df.loc[((df['HS']>df['HS'].shift())&(df['LT']>5)),'HS']=None
    return df

def LT_processing(df):
    """ Clear bad air temperature from raw data """
    print('Tidying air temp data')
    df.loc[(df['LT']<=-30),'LT']=None
    df.loc[(df['LT']>=35),'LT']=None
    df.loc[((df['LT']-df['LT'].shift())>5),'LT']=None
    df.loc[((df['LT']-df['LT'].shift())<-5),'LT']=None
    return df

def N_processing(df):
    """ Clear bad precepitation measurments from raw data"""
    print('Remove negative precepitation measurments')
    df.loc[(df['N']<0),'N'] = None
    return df

def LF_processing(df):
    """ Clear bad air humidity measurments from raw data"""
    print('Remove bad air humidtiy measurments from raw data')
    df.loc[(df['LF']<0),'LF'] = None
    return df

def GS_processing(df):
    """ Clear bad solar radiation measurments from raw data"""
    print('Remove bad solar radiation measurments from raw data')
    df.GS = df.GS * 0.0006
    df.loc[(df['GS']<0),'GS'] = None
    
    return df

def SD_processing(df):
    """ Clear bad sun shine measurments from raw data"""
    print('Remove bad sun shine measurments from raw data')
    df.SD = df.SD / 3600
    df.loc[(df['SD']<0),'SD'] = None
    print('Convert seconds in hours')
    return df




def TD_calc(station):
    """ Function to calculate the temperature difference for a certain station
    station: string"""
    df=dd.ReadfromProcessed(station,'Monthly',sensor='LT')
    df=df.set_index('Datum')
    TD_ls = []
    for i in range(df.index[0].year,df.index[len(df.index)-1].year):
        df_year=df.loc[df.index.year == i]
        TD_year = df_year.LT.max() - df_year.LT.min()
        if pd.isna(TD_year)!= True:
            TD_ls.append(TD_year)

    TD = sum(TD_ls) / len(TD_ls)
    return TD

def TD_calc_specific(station,datum):
    """Function to calculate the temperature difference for a certain station in a specific year
    station: string
    datum: date string  with format YYYYMMDD"""
    datum=datetime.datetime.strptime(datum,'%Y%m%d')
    df=dd.ReadfromProcessed(station,'Monthly',sensor='LT')
    df=df.set_index('Datum')
    df_year=df.loc[df.index.year == datum.year-1]
    TD = df_year.LT.max() - df_year.LT.min()
    return TD

def PPTWT_calc(station):
    """ Function to calculate the winter precipitation for a certain station
    station: string"""
    df=dd.ReadfromProcessed(station,'Monthly',sensor='N')
    df=df.set_index('Datum')
    if df.N.sum() == 0:
        print('This station has no precip data choose Wolkenstein/St. Martin/St. Veit')
        station = input('Enter new station name: ')
        df=dd.ReadfromProcessed(station,'Monthly',sensor='N')
        df=df.set_index('Datum')
    PPTWT_ls = []
    for i in range(df.index[0].year,df.index[len(df.index)-1].year):
        df_wint=df.loc[((df.index.month == 12) & (df.index.year == i)) | ((df.index.month == 1) & (df.index.year == i+1)) | ((df.index.month == 2)&(df.index.year == i+1))]
        if pd.isna(df_wint.N.sum())!=True:
            PPTWT_ls.append(df_wint.N.sum())

    PPTWT = sum(PPTWT_ls)/len(PPTWT_ls)
    return PPTWT

def PPTWT_calc_specific(station,datum):
    """ Function to calculate the winter precipitation for a certain station in a specific year
    station: string"""
    datum=datetime.datetime.strptime(datum,'%Y%m%d')
    df=dd.ReadfromProcessed(station,'Monthly',sensor='N')
    df=df.set_index('Datum')
    if df.N.sum() == 0:
        print('This station has no precip data choose Wolkenstein/St. Martin/St. Veit')
        #station = input('Enter new station name: ')
        df=dd.ReadfromProcessed('Wolkenstein','Monthly',sensor='N')
        df=df.set_index('Datum')
    df_wint=df.loc[((df.index.month == 12) & (df.index.year == datum.year-1)) | ((df.index.month == 1) & (df.index.year == datum.year)) | ((df.index.month == 2)&(df.index.year == datum.year))]
    PPTWT=df_wint.N.sum()
    return PPTWT

def DOY_calc(datum):
    """" Calculates the day of year
    datum: date string with format YYYYMMDD"""
    datum=datetime.datetime.strptime(datum,'%Y%m%d')
    DOY = datetime.date.toordinal(datum)-datetime.date.toordinal(datetime.date(datum.year,9,30))
    if DOY < 0:
        DOY = DOY+365
    return DOY

def SWE_calc(station,datum):
    """ Calculates the snow-water-equivalent at certain date and station
    station: string
    datum: date string with format YYYYMMDD"""

    TD=TD_calc(station)
    PPTWT = PPTWT_calc(station)
    df = dd.ReadfromProcessed(station,'Daily',sensor='HS',startdate=datum,enddate=datum)
    H= df.HS[0] * 10
    DOY = DOY_calc(datum)
    print('Snowheight(mm): ',H)
    print('TD: ',TD)
    print('PPTWT: ',PPTWT)

    a = [0.0533,0.948,0.1701,-0.1314,0.2922] #accumulation phase
    b = [0.0481,1.0395,0.1699,-0.0461,0.1804] #ablation phase
    SWE = a[0]*H**a[1]*PPTWT**a[2]*TD**a[3]*DOY**a[4]* \
    (-np.tanh(.01*(DOY-180))+1)/2 + b[0]*H**b[1]* \
    PPTWT**b[2]*TD**b[3]*DOY**b[4] * (np.tanh(.01*(DOY-180))+1)/2
    return SWE

def SWE_calc_period(station,station_n,startdate='',enddate=''):
    """ Calculates SWE between two dates
    station: string
    station_n: string (must be station with precip)
    startdate: string (format: YYYYMMDD)
    enddate: string (format: YYYYMMDD)"""

    TD=TD_calc(station)
    PPTWT = PPTWT_calc(station_n)
    #TD=TD_calc_specific(station,datum)
    #PPTWT = PPTWT_calc_specific(station,datum)
    print('TD: ',TD)
    print('PPTWT: ',PPTWT)

    df = dd.ReadfromProcessed(station,'Daily',sensor='HS,LT',startdate=startdate,enddate=enddate)
    print(len(df))
    #df=df.dropna()
    print(len(df))
    df=df.set_index('Datum')
    SWE_ls=[]
    for i in range(0,len(df.index)):
        DOY = DOY_calc(datetime.datetime.strftime(df.index[i],"%Y%m%d"))
        H= df.HS[i] * 10
        a = [0.0533,0.948,0.1701,-0.1314,0.2922] #accumulation phase
        b = [0.0481,1.0395,0.1699,-0.0461,0.1804] #ablation phase
        SWE = a[0]*H**a[1]*PPTWT**a[2]*TD**a[3]*DOY**a[4]* \
        (-np.tanh(.01*(DOY-180))+1)/2 + b[0]*H**b[1]* \
        PPTWT**b[2]*TD**b[3]*DOY**b[4] * (np.tanh(.01*(DOY-180))+1)/2
        SWE_ls.append(SWE)
    df['SWE']=SWE_ls
    return df


def SWE_calc_specific(station,datum):
    """Calculates SWE at a specific date
    station: string
    datum: string (format: YYYYMMDD)"""

    #TD=TD_calc(station)
    #PPTWT = PPTWT_calc(station)
    TD=TD_calc_specific(station,datum)
    PPTWT = PPTWT_calc_specific(station,datum)
    df = dd.ReadfromProcessed(station,'Daily',sensor='HS',startdate=datum,enddate=datum)
    H= df.HS[0] * 10
    DOY = DOY_calc(datum)
    print('Snowheight(mm): ',H)
    print('TD: ',TD)
    print('PPTWT: ',PPTWT)


    a = [0.0533,0.948,0.1701,-0.1314,0.2922] #accumulation phase
    b = [0.0481,1.0395,0.1699,-0.0461,0.1804] #ablation phase
    SWE = a[0]*H**a[1]*PPTWT**a[2]*TD**a[3]*DOY**a[4]* \
    (-np.tanh(.01*(DOY-180))+1)/2 + b[0]*H**b[1]* \
    PPTWT**b[2]*TD**b[3]*DOY**b[4] * (np.tanh(.01*(DOY-180))+1)/2

    return SWE


def extraterrestrial_rad(df):
    """ Calculates extraterrestrial radiation from a dataframe with datetime column
    df: DataFrame"""

    lat = 46.6792 * np.pi / 180
    df['Day_of_Month']=df.Datum.dt.month
    df['DaylightHours']= 4*lat*np.sin(0.53*df.Day_of_Month-1.65) + 12
    df['Ra']=3*df.DaylightHours * np.sin(0.131 * df.DaylightHours - 0.95 * lat)
    return df


def evapo_penman_simple():
    """ Calculates the simplified penman equatin for direct lake evaporation"""

    df_GS = dd.ReadfromProcessed('St. Martin','Daily',sensor='GS')
    df_LT = dd.ReadfromProcessed('Rossalm','Daily',sensor='LT,LF')
    df = df_GS.join(df_LT.LT)
    df = df.join(df_LT.LF)
    df=df.dropna()
    df = extraterrestrial_rad(df)
    df['Epen'] = 0.047 * df.GS * np.sqrt(df.LT+9.5)-2.4*(df.GS/df.Ra)**2+0.09*(df.LT + 20)*(1-(df.LF/100))
    return df



def polyfunc(x,y,degree):
    """ Function to fit a polynominal curve with up to three degrees
    x: array
    y: array
    degree: int"""
    z,res,_,_,_ = np.polyfit(x,y,degree,full=True)
    f = np.poly1d(z)
    x_new = np.linspace(x.iat[0],x.iat[-1],50)
    y_new = f(x_new)
    if degree > 3:
        print('This function only supports up to 3 degrees')
        return
    if degree == 1:
        poly_func = ((str(round(z[0],4)) + 'x + ' + str(round(z[1],4))).replace('.',',')).replace('+-','-')
    if degree == 2:
        poly_func = ((str(round(z[0],4)) + 'x² + ' + str(round(z[1],4)) + 'x + ' + str(round(z[2],4))).replace('.',',')).replace('+-','-')
    if degree == 3:
        poly_func = ((f"{z[0]:.3}" + 'x³ + ' + f"{z[1]:.3}" + 'x² + ' + f"{z[2]:.3}" + 'x + ' + f"{z[3]:.3}").replace('.',',')).replace('+ -','-')
    coefs=[c for c in z]
    return x_new,y_new,poly_func,coefs,res

def calc_from_polyfunc(x,coefs,grade=2):
    """ Calculates any x value of a polynominal function with coefs
    x: array
    coefs: list
    grade: int"""
    if grade == 1:
        return coefs[0]*x+coefs[1]
    if grade == 2:
        return coefs[0]*x**2+coefs[1]*x+coefs[2]
    if grade == 3:
        return coefs[0]*x**3+coefs[1]*x**2+coefs[2]*x+coefs[3]


def calcAreaFromDepth(depth,lake):
    """ Calculates area from depth for Lake Limo and Lake Parom
    depth: float
    lake: string(Limo or Parom)"""
    if lake == 'Limo':
        return 41.05090849623958 * depth**3 - 876.9004337585678*depth**2+7252.335363659846*depth-9634.640993941795
    if lake == 'Parom':
        return 3049.762182195784 * depth - 3382.009706965325

def calcEvapoAreaLimo():
    """ Calculates direct lake evaporation from Lake Limo"""
    df = pd.read_csv('LoggerData/Limo_calcs_measurements_new.csv')
    df=df.sort_values('Datum')
    df.loc[pd.isna(df.Area_glatt)==True,['Area_glatt']]=calcAreaFromDepth(df.WD,'Limo')
    df=df.loc[pd.isna(df.WD)==False]
    df_evapo = evapo_penman_simple()
    df_evapo = df_evapo.set_index('Datum')
    df.Datum = pd.to_datetime(df.Datum)
    df = df.set_index('Datum')
    df=df.loc[(pd.isna(df.WD)==False)&(df.WD != 11)]
    df['Epen_area'] = [Area * df_evapo.Epen[df.index[n]] for n,Area in enumerate(df.Area_glatt)]
    df['Epen_shed'] = (df.Epen_area/849285)*-1
    df.loc[df.index.month.isin([6,7,8,9,10])==False,['Epen_area']]=np.nan
    return df


def calculate_inflow_simple(high,low,seepage):
    """ Inflow calculation based on highstand and lowstand
    high: list/tuple
    low: list/tuple
    seepage: float"""
    time_in_seconds=(datetime.datetime.strptime(low[0],"%Y-%m-%d")-datetime.datetime.strptime(high[0],"%Y-%m-%d")).total_seconds()
    print(time_in_seconds)
    return (((high[1] - low[1])+((seepage/1000)*time_in_seconds))/time_in_seconds)*1000

def calculate_seepage_simple(high,low):
    """ Seepage calculation based on highstand and lowstand
    high: list/tuple
    low: list/tuple"""
    return ((high[1] - low[1])/(datetime.datetime.strptime(low[0],"%Y-%m-%d")-datetime.datetime.strptime(high[0],"%Y-%m-%d")).total_seconds())*1000


def calcEvapoAreaParom():
    """ Calculates direct lake evaporation from Lake Parom"""
    df = pd.read_csv('LoggerData/Parom_calcs_measurements_new.csv')
    df=df.sort_values('Datum')
    df.loc[pd.isna(df.Area_glatt)==True,['Area_glatt']]=calcAreaFromDepth(df.WD,'Parom')
    df=df.loc[pd.isna(df.WD)==False]
    df_evapo = evapo_penman_simple()
    df_evapo = df_evapo.set_index('Datum')
    df.Datum = pd.to_datetime(df.Datum)
    df = df.set_index('Datum')
    df=df.loc[(pd.isna(df.WD)==False)]
    df['Epen_area'] = [Area * df_evapo.Epen[df.index[n]] for n,Area in enumerate(df.Area_glatt)]
    df['Epen_shed'] = (df.Epen_area/8039068)*-1
    df.loc[df.index.month.isin([6,7,8,9,10])==False,['Epen_area']]=np.nan
    return df



def water_balance_limo():
    """ Testing the equation: R+S-GWout-AET=Ls+SSGW for Lake Limo"""
    limo_file_path = 'LoggerData/Limo_WD_Vol_change.csv'
    Ls = pd.read_csv(limo_file_path,index_col='Datum')
    Ls['W_change_shed'] = (Ls.W_change * 1000)/849285
    N = dd.ReadfromProcessed('Wolkenstein','Daily',sensor='N',startdate=str(Ls.index[0]).replace('-','')[:8],enddate=str(Ls.index[-1]).replace('-','')[:8])
    SWE = SWE_calc_period('Rossalm','Wolkenstein',startdate=str(Ls.index[0]).replace('-','')[:8],enddate=str(Ls.index[-1]).replace('-','')[:8])
    SWE['snowmelt'] = [SWE.SWE[n] - SWE.SWE[n+1] if SWE.SWE[n] > SWE.SWE[n+1] else 0 for n in range(len(SWE.SWE)-1)]+[np.nan]
    N['ET_12'] = N.N-(N.N * 0.12)
    GWout = round((9.1 * 86400)/849285,2)
    N.set_index('Datum',inplace=True)
    data_dict = {datum:[N.ET_12[datum] + SWE.snowmelt[datum] - GWout - Ls.W_change_shed[datum]]for datum in Ls.index}
    df_wb = pd.DataFrame.from_dict(data=data_dict, orient='Index',columns=['SSGW'])
    df_wb.index = pd.to_datetime(df_wb.index)
    df_wb['N'] = N.ET_12
    df_wb['snowmelt']=SWE.snowmelt
    df_wb['lakestorage']=Ls['W_change_shed']
    return df_wb