import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import pandas as pd 
import numpy as np 
import DownloadDatabase as dd
import matplotlib.dates as mdates
from adjustText import adjust_text
import os
import locale
import datetime
from matplotlib.ticker import (MultipleLocator, AutoMinorLocator,LinearLocator)
import Processing as pc
from sklearn.mixture import GaussianMixture
from sklearn.neighbors import KernelDensity
from scipy import stats
import matplotlib.patheffects as PathEffects
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

locale.setlocale(locale.LC_ALL,'de_DE.utf8')
plt.rcParams['axes.formatter.use_locale']=True

def width_bar_plot(rule,df):
    """ Sets the width for a bar plot based on the rule
    rule: string
    df: dataframe"""
    if rule == 'Hourly':
        return 1/24
    if rule == 'Daily':
        return 1
    if rule == 'Weekly':
        return 7
    if rule == 'Monthly':
        return [-df['Datum'][n].day for n in range(len(df['Datum']))]


def Yearly_N_sum(station='Wolkenstein'):
    """Return yearly precip values at a certain station
    station: string"""
    df=dd.ReadfromProcessed(station,'Monthly',sensor='N')
    df=df.set_index('Datum')
    df=df.resample('Y').sum()
    #df.index = pd.date_range(start=pd.datetime(1998,1,1),end=pd.datetime(2020,1,1),freq='AS')
    return df

def plot_snow_height_monthly(xmargin_l=0.01,xmargin_r=0.01,legpos=(0.68,0.86),size=(16,8)):
    """ Plot monthly snowheight
    xmargin_l: float
    xmaring_r: float
    legpos: tuple
    size: tuple
    """
    plt.rcParams.update({'font.size':8})
    stations=['Rossalm','Piz La Ila']
    rule='Monthly'
    df_comb = pd.DataFrame(data=None,index=None,columns={'HS','ID','Datum'})
    for s in stations:
        df_comb=df_comb.append(dd.ReadfromProcessed(s,rule,sensor='HS'))
    df_N = Yearly_N_sum()
    df_N=df_N.loc[df_N.ID ==48]

    fig, ax=plt.subplots(figsize=cm2inch(size))
    ax1 = ax.twinx()
    ax.set_zorder(1)
    ax1.set_zorder(0.5)
    ax.patch.set_visible(False)

    

    ax.plot(df_comb.loc[df_comb.ID==2].Datum,df_comb.loc[df_comb.ID==2].HS,c='darkorange',label='Rossalm')
    ax.plot(df_comb.loc[df_comb.ID==1].Datum,df_comb.loc[df_comb.ID==1].HS,c='royalblue',label='Piz La Ila')

    ax1.bar(df_N.index,df_N.N,width=365,align='edge',color='lightgrey',alpha=0.5,label="Niederschlag Wolkenstein")

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    fig.autofmt_xdate()

    set_xmargin(ax1,left=xmargin_l,right=xmargin_r)
    set_xmargin(ax,left=xmargin_l,right=xmargin_r)

    ax.set_ylabel(ylabel('HS'))
    ax1.set_ylabel(ylabel('N'))

    fig.legend(loc=legpos)
    plt.tight_layout()



    


def LoadLavarellaQuelleExcel(path='',filename='',sheetname='',rule='Daily',resample=True):
    """ Load spring data from excel"""
    rule, rule_ac = dd.Rules(rule)
    if path == '':
        path = 'LoggerData'
    if filename == '':
        filename = 'LF Logger Lavarella unten_T93_200707.xlsx'
    if sheetname == '':
        sheetname = 'T93'
    
    df = pd.read_excel(os.path.join(path,filename),sheet_name=sheetname)

    df = df.drop(columns=df.columns[1:3])
    df = df.rename(columns={'Temp, °C':'WT','Specific Conductance, μS/cm':'SLT','Date Time, GMT+02:00':'Datum'})
    df = df.set_index('Datum')
    if resample == True:
        df = df.resample(rule=rule_ac).mean()

    return df
    
def set_xmargin(ax, left=0.0, right=0.3):
    """ Adjusts the xmargin in a plot"""
    ax.set_xmargin(0)
    lim = ax.get_xlim()
    delta = np.diff(lim)
    left = lim[0] - delta*left
    right = lim[1] + delta*right
    ax.set_xlim(left,right)



def Plot_LT_SLT_N(df='',left='',right='',rule='',xmargin=0.09,height=-0.8,minter=2,legpos=(0.68,0.86),n_station='Wolkenstein',roll=24):
    """ Plot temp, specific conductance and precip"""
    plt.rcParams.update({'font.size':8})
    if rule == '':
        rule = 'Daily'
    if df == '':
        df = LoadLavarellaQuelleExcel(rule=rule)

    df_N = dd.ReadfromProcessed(n_station,rule,sensor='N',startdate=str(df.index[0]).replace('-','')[0:8],enddate=str(df.index[-1]).replace('-','')[0:8])
    if left == '' and right!='':
        left = df.index[0]
    if left != '' and right == '':
        right = df.index[-1]
    if left == '' and right == '':
        right = df.index[-1]
        left = df.index[0]

    if rule == "Hourly":
        df['SLT_rol'] = df.SLT.rolling(roll,center=True).mean()
        df['WT_rol'] = df.WT.rolling(roll,center=True).mean()

    df = df.loc[(df.index>left)&(df.index<right)]
    df_N = df_N.loc[(df_N.Datum>left)&(df_N.Datum<right)]    

    fig, ax = plt.subplots(2,1,figsize=cm2inch(16,8))
    fig.subplots_adjust(hspace=height)

    ax2 = ax[1]
    ax = ax[0]
    ax1 = ax.twinx()

    if rule != "Hourly":
        ax.plot(df.index, df.WT,c='slateblue',label='Wassertemperatur')
        ax1.plot(df.index,df.SLT,c='firebrick',label='Spezifische Leitfähigkeit',alpha=0.8)
    
    ax2.bar(df_N.Datum,df_N.N,width=width_bar_plot(rule,df_N),align='edge',color='grey',label='Niederschlag '+n_station,alpha=0.8)
    
    if rule == "Hourly":
        ax.plot(df.index, df.WT,c='slateblue',label='Wassertemperatur',alpha=0.6)
        ax1.plot(df.index,df.SLT,c='firebrick',label='Spezifische Leitfähigkeit',alpha=0.6)
        ax1.plot(df.index,df['SLT_rol'],c='firebrick')
        ax.plot(df.index,df['WT_rol'],c='slateblue')
    
    set_xmargin(ax2,left=0,right=xmargin)
    set_xmargin(ax,left=0,right=xmargin)
    
    for x in fig.get_axes():
        x.spines['top'].set_visible(False)
        #set_xmargin(x,left=0.01,right=xmargin)
        #if left != '' and right != '':
            #x.set_xlim(left=mdates.datestr2num(left),right=mdates.datestr2num(right))
        if x == [s for s in fig.get_axes()][0] or x == [s for s in fig.get_axes()][2]:
            x.spines['bottom'].set_visible(False)
            x.spines['bottom'].set_visible(False)
            x.xaxis.set_visible(False)

    


    ax2.patch.set_visible(False)
    if rule != "Hourly":
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=minter))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    else:
        ax2.xaxis.set_major_locator(mdates.HourLocator(interval=minter))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
        ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=minter//2))
    ax.yaxis.set_label_text('Leitfähigkeit [\u03BCS/cm]')
    ax.yaxis.set_label_coords(x=0.935,y=0.5)
    ax1.yaxis.set_label_text('Wassertemperatur [°C]')
    ax1.yaxis.set_label_coords(x=1.1,y=0.5)
    ax2.yaxis.set_label_text('Niederschlag [mm]')
    ax2.yaxis.set_label_coords(x=-0.05,y=0.68)

    ax.yaxis.set_tick_params(direction='out',left=False,right=True,labelleft=False,labelright=True)
    ax1.yaxis.set_tick_params(direction='in',pad=-5)
    ax1.set_yticklabels([int(x) for x in ax1.get_yticks().tolist()] ,ha='right')
    fig.legend(loc=legpos)
    fig.autofmt_xdate()



def read_excel(filename,sheetname,path=''):
    """ Read excel data
    filename: string
    sheetname: string
    path: path"""
    if path == '':
        path = 'LoggerData'
    df = pd.read_excel(os.path.join(path,filename),sheet_name=sheetname)
    return df

def DropRenameResample(df,cols=[0,6],rule='Daily'):
    """ Drop, rename and resample a dataframe loaded from the excel file
    df: dataframe
    cols: list
    rule: string"""
    col_dict = {c[0]:c[1] for c in enumerate(df.columns)}
    col_int = [x for x,y in col_dict.items()]
    if cols == 'Select':
        for k,v in col_dict.items():
            print(k,'--',v)
        remove= [int(x) for x in input('Chose columns to keep').split(',')]
        cols = [x for x in col_int if x not in remove]
    else:
        cols = [x for x in col_int if x not in cols]

    df = df.drop(df.columns[cols],axis=1)

    rename_dict = {c[1]:'' for c in enumerate(df.columns)}
    standard_rename_dict = {'Date Time, GMT+02:00':'Datum','Temp, °C':'WT','T2 (m)':'WD'}
    for k,v in rename_dict.items():
        if k in standard_rename_dict:
            rename_dict[k] = standard_rename_dict[k]
    for k,v in rename_dict.items():
        if v == '':
            rename_dict[k] = input(k+' column empty enter column name')
    df = df.rename(columns=rename_dict)
            

    
    rule, rule_ac = dd.Rules(rule)
    df = df.set_index('Datum')
    df = df.resample(rule_ac).mean()

    return df 

def Plot_Lake_Level(lake='Limo',left='',right='',rule='Daily',xmargin_r=0.01,xmargin_l=0.01,height=-0.8,minter=2,legpos=(0.68,0.86),temp=False,size=(16,10),h_padding=-20):
    """ Plot lake level"""
    plt.rcParams.update({'font.size':8})

    if lake == 'Limo':
        df_u = read_excel('Logger Limo.xlsx','T100, dann T101')
        df_o = read_excel('Logger Limo.xlsx','T98, dann T102')
    if lake == 'Parom':
        df_u = read_excel('Logger Parom.xlsx','T97')
        df_o = read_excel('Logger Parom.xlsx','T96')

    if temp == False:
        df_u = DropRenameResample(df_u)
        df_o = DropRenameResample(df_o)
        axes_count=2
    else:
        df_u = DropRenameResample(df_u,cols=[0,2,6])
        df_o = DropRenameResample(df_o,cols=[0,2,6])
        axes_count=3

    df_N = dd.ReadfromProcessed('Wolkenstein',rule,sensor='N',startdate=str(df_u.index[0]).replace('-','')[0:8],enddate=str(df_u.index[-1]).replace('-','')[0:8])
    



    if left == '' and right=='':
        left = df_u.index[0]
        right = df_u.index[-1]
    else:
        if left == '':
            left = df_u.index[0]
        if right== '':
            right = df_u.index[-1]

    df_u = df_u.loc[(df_u.index>left)&(df_u.index<right)]
    df_o = df_o.loc[(df_o.index>left)&(df_o.index<right)]
    df_N = df_N.loc[(df_N.Datum>left)&(df_N.Datum<right)]
    plt.rcParams.update({'font.size':8})
    fig, ax = plt.subplots(axes_count,1,figsize=cm2inch(size))
    fig.subplots_adjust(hspace=height)

    
    
    if temp != False:
        ax1 = ax[0]
        ax2 = ax[2]
        ax = ax[1]
    else:
        ax2 = ax[1]
        ax = ax[0]
    

    ax.plot(df_u.index, df_u.WD,c='b',label='UL')
    ax.plot(df_o.index,df_o.WD,c='firebrick',label='OL',alpha=0.8)
    if temp != False:
        ax1.plot(df_u.index, df_u.WT,c='lightblue')
        set_xmargin(ax1,left=xmargin_l,right=xmargin_r)
    ax2.bar(df_N.Datum,df_N.N,width=width_bar_plot(rule,df_N),align='edge',color='grey',label='N',alpha=0.8)
    
    set_xmargin(ax2,left=xmargin_l,right=xmargin_r)
    set_xmargin(ax,left=xmargin_l,right=xmargin_r)

    
    for x in fig.get_axes():
        x.spines['top'].set_visible(False)
        if x != [s for s in fig.get_axes()][-1]:
            x.spines['bottom'].set_visible(False)
            x.xaxis.set_visible(False)

    


    ax2.patch.set_visible(False)
    ax.patch.set_visible(False)

    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=minter))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    if temp != False:
        ax1.yaxis.set_tick_params(direction='in',pad=-10,left=True,right=False,labelleft=True,labelright=False)
        ax1.set_yticklabels([int(x) for x in ax1.get_yticks().tolist()] ,ha='left')
        ax1.patch.set_visible(False)
        ax1.text(0.04, 0.5, 'Wassertemperatur [°C]', rotation=90,
            ha='left',
            va='center', 
            transform=ax1.transAxes)

    ax.text(1.05,0.5,'Seespiegel [m]',rotation=90,ha='left',
    va='center',
    transform=ax.transAxes)
    ax2.set_ylabel('Niederschlag [mm]')

    ax.yaxis.set_tick_params(direction='out',left=False,right=True,labelleft=False,labelright=True)
    fig.legend(loc=legpos)
    fig.autofmt_xdate()
    fig.tight_layout(h_pad=h_padding)




def parom_poly(x):
    return 1372.99*x**2-2808.53*x+1728.66
def limo_poly(x):
    return 931.46*x**2-739.07*x+698.83

def Plot_Lake_Volume(lake='Limo',left='',right='',rule='Daily',xmargin_r=0.01,xmargin_l=0.01,height=-0.8,minter=2,legpos=(0.68,0.86),temp=False,size=(10,5),h_padding=-12):
    """ Plot lake volume"""
    plt.rcParams.update({'font.size':8})

    if lake == 'Limo':
        df_u = read_excel('Logger Limo.xlsx','T100, dann T101')
        #df_o = read_excel('Logger Limo.xlsx','T98, dann T102')
        vol_label='Volumen Limosee'
    if lake == 'Parom':
        df_u = read_excel('Logger Parom.xlsx','T97')
        #df_o = read_excel('Logger Parom.xlsx','T96')
        vol_label='Volumen Paromsee'

    if temp == False:
        df_u = DropRenameResample(df_u)
        #df_o = DropRenameResample(df_o)
        axes_count=2
    else:
        df_u = DropRenameResample(df_u,cols=[0,2,6])
        #df_o = DropRenameResample(df_o,cols=[0,2,6])
        axes_count=3

    if lake == 'Limo':
        df_u['Vol'] = limo_poly(df_u.WD)
    if lake == 'Parom':
        df_u['Vol'] = parom_poly(df_u.WD)

    df_N = dd.ReadfromProcessed('Wolkenstein',rule,sensor='N',startdate=str(df_u.index[0]).replace('-','')[0:8],enddate=str(df_u.index[-1]).replace('-','')[0:8])
    



    if left == '' and right=='':
        left = df_u.index[0]
        right = df_u.index[-1]
    else:
        if left == '':
            left = df_u.index[0]
        if right== '':
            right = df_u.index[-1]

    df_u = df_u.loc[(df_u.index>left)&(df_u.index<right)]
    #df_o = df_o.loc[(df_o.index>left)&(df_o.index<right)]
    df_N = df_N.loc[(df_N.Datum>left)&(df_N.Datum<right)]

    plt.rcParams.update({'font.size':8})
    fig, ax = plt.subplots(axes_count,1,figsize=cm2inch(size))
    fig.subplots_adjust(hspace=height)

    
    
    if temp != False:
        ax1 = ax[0]
        ax2 = ax[2]
        ax = ax[1]
    else:
        ax2 = ax[1]
        ax = ax[0]
    

    ax.plot(df_u.index, df_u.Vol,c='b',label=vol_label)
    #ax.plot(df_o.index,df_o.Vol,c='firebrick',label='Oberer Logger',alpha=0.8)
    if temp != False:
        ax1.plot(df_u.index, df_u.WT,c='lightblue')
        #ax1.plot(df_o.index, df_o.WT,c='r')
        set_xmargin(ax1,left=xmargin_l,right=xmargin_r)
    ax2.bar(df_N.Datum,df_N.N,width=width_bar_plot(rule,df_N),align='edge',color='grey',label='Niederschlag Wolkenstein',alpha=0.8)
    
    set_xmargin(ax2,left=xmargin_l,right=xmargin_r)
    set_xmargin(ax,left=xmargin_l,right=xmargin_r)

    
    for x in fig.get_axes():
        x.spines['top'].set_visible(False)
        #set_xmargin(x,left=0.01,right=xmargin)
        #if left != '' and right != '':
            #x.set_xlim(left=mdates.datestr2num(left),right=mdates.datestr2num(right))
        if x != [s for s in fig.get_axes()][-1]:
            x.spines['bottom'].set_visible(False)
            x.xaxis.set_visible(False)

    


    ax2.patch.set_visible(False)
    ax.patch.set_visible(False)

    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=minter))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    if temp != False:
        #ax1.yaxis.set_label_text('Wassertemperatur [m]')
        #ax1.yaxis.set_label_coords(x=0.075,y=0.5)
        ax1.yaxis.set_tick_params(direction='in',pad=-10,left=True,right=False,labelleft=True,labelright=False)
        ax1.set_yticklabels([int(x) for x in ax1.get_yticks().tolist()] ,ha='left')
        ax1.patch.set_visible(False)
        ax1.text(0.04, 0.5, 'Wassertemperatur [°C]', rotation=90,
            ha='left',
            va='center', 
            transform=ax1.transAxes)

    ax.text(1.10,0.5,'Volumen [m³]',rotation=90,ha='left',
    va='center',
    transform=ax.transAxes)
    #ax.yaxis.set_label_text('Seespiegel [m]')
    #ax.yaxis.set_label_coords(x=1.085,y=0.5)
    #ax2.text(0,0,'Niederschlag [mm]',rotation=90,va='center',ha='center',transform=ax2.transAxes)
    #ax2.yaxis.set_label_text('Niederschlag [mm]')
    #ax2.yaxis.set_label_coords(x=-0.05,y=0.64)
    ax2.set_ylabel('Niederschlag [mm]')

    ax.yaxis.set_tick_params(direction='out',left=False,right=True,labelleft=False,labelright=True)
    fig.legend(loc=legpos)
    fig.autofmt_xdate()
    fig.tight_layout(h_pad=h_padding)




def plot_vol_with_calc(lake='Limo',rule='Daily',
                        minter=2,legpos=(0.68,0.86),size=(16,10),
                        vol_lim=(-10000,120000),N_lim=(0,100),use_swe=False):
    """ Plot lake volume with calculated volume"""
    plt.rcParams.update({'font.size':8})

    if lake == 'Limo':
        path_limo = 'LoggerData/Limo_calcs_measurements_new.csv'
        df_comb = pd.read_csv(os.path.join(path_limo))
        df_comb['Datum'] = pd.to_datetime(df_comb.Datum)
        df_comb = df_comb.set_index('Datum')
        vol_label= 'Vol'
        vol_label_calc = 'Vol'
    if lake == 'Parom':
        path_parom = 'LoggerData/Parom_calcs_measurements_new.csv'
        df_comb = pd.read_csv(os.path.join(path_parom))
        df_comb['Datum'] = pd.to_datetime(df_comb.Datum)
        df_comb = df_comb.set_index('Datum')
        vol_label= 'Vol'
        vol_label_calc = 'Vol'

    df_N = dd.ReadfromProcessed('Wolkenstein',rule,sensor='N',startdate="20141001",enddate="20200601")
    df_N = df_N.set_index('Datum')
    swe = cp.SWE_calc_period('Rossalm','Wolkenstein','20141001',"20200601")



    if use_swe == True:
        swe_dict = {str(year) +'-'+ str(year+1):swe[str(year)+'-10':str(year+1)+'-06'] for n,year in enumerate(swe.index.year.unique()) if year < 2020}
        for k,v in swe_dict.items():
            swe_dict[k]['SWE_sum'] = [0]+[v.SWE[n+1] - v.SWE[n] if  (v.SWE[n+1] > v.SWE[n]) & (v.SWE[n+1] - v.SWE[n] > 0 ) else 0 for n in range(len(v.SWE)-1)]
            swe_dict[k]['SWE_cumsum'] = swe_dict[k]['SWE_sum'].cumsum()
    else:
        snow_dict = {str(year) +'-'+ str(year+1):swe[str(year)+'-10':str(year+1)+'-06'] for n,year in enumerate(swe.index.year.unique()) if year < 2020}
        for k,v in snow_dict.items():
            snow_dict[k]['snow_sum'] = [0]+[v.HS[n+1] - v.HS[n] if  (v.HS[n+1] > v.HS[n]) & (v.HS[n+1] - v.HS[n] > 0 ) else 0 for n in range(len(v.HS)-1)]
            snow_dict[k]['snow_cumsum'] = snow_dict[k]['snow_sum'].cumsum()


    fig, ax = plt.subplots(figsize=cm2inch(size),sharex=True)

    ax1 = ax.twinx()
    ax2 = ax.twinx()

    ax2.plot(df_comb.index, df_comb.Vol_calc,'+',color='darkorange',label=vol_label_calc)
    ax2.plot(df_comb.index, df_comb.Volume,c='darkslateblue',label=vol_label)
    

    ax1.bar(df_N.index,df_N.N,width=1,align='edge',color='black',label='N',alpha=0.8)

    if use_swe == True:
        for k,v in swe_dict.items():
            ax.bar(v.index,v.SWE_cumsum,width=1,color='xkcd:dark teal',alpha=0.2,label='SWE')
    else:
        for k,v in snow_dict.items(): 
            ax.bar(v.index,v.snow_cumsum,width=1,color='xkcd:dark teal',alpha=0.2,label='HS')



    for axes in fig.axes:
        axes.set_xlim(datetime.date(2014,5,1),datetime.date(2020,7,1)+datetime.timedelta(days=30))
        axes.xaxis.set_major_locator(mdates.YearLocator())
        axes.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
        axes.xaxis.set_minor_locator(mdates.MonthLocator(interval=minter))
    



    ax2.set_ylabel('Volumen [m³]')
    ax2.yaxis.set_label_position('right')
    ax1.set_ylabel('Niederschlag [mm]',labelpad=-25)
    ax1.yaxis.set_label_position('left')
    ax1.yaxis.set_tick_params(direction='in',left=True,right=False,labelleft=True,labelright=False,pad=-15)

    ax1.set_yticks(ax1.get_yticks()[1:-1])
    ax2.set_yticks(ax2.get_yticks()[1:])
    ax.set_ylabel('kumulative Schneehöhe [cm]')

    ax2.set_ylim(vol_lim)
    ax.set_ylim(N_lim)

    hand = [ax.get_legend_handles_labels()[0][0],ax1.get_legend_handles_labels()[0][0],ax2.get_legend_handles_labels()[0][0],ax2.get_legend_handles_labels()[0][1]]
    ax.legend(handles=hand,loc=legpos)
    
    fig.autofmt_xdate()
    plt.tight_layout()



def plot_isotopes(Q='',size=(16,5),legpos=(0.5,0.5)):
    """ Plot the isotope data"""
    df = pd.read_csv('IsotopeData/Isotope_data_Fanes_all.csv',parse_dates=['Datum'])
    
    fig, ax = plt.subplots(1,1,figsize=cm2inch(size))
    if Q == "LimoseeNord":
        q_label = "Limosee"
    else:
        q_label = Q

    
    

    ax.plot(df['d18O'].loc[df.Quelle==Q],df.d2H.loc[df.Quelle==Q],'o',label=q_label)

    ax.errorbar(df['d18O'].loc[df.Quelle==Q],
                df.d2H.loc[df.Quelle==Q],
                xerr=df['s.d. d18O'].loc[df.Quelle==Q],
                yerr=df['s.d. d2H'].loc[df.Quelle==Q],
                fmt='none',
                elinewidth=1,
                ecolor='black',
                capsize=2,
                capthick=0.2,
                label='SD')

    texts=[]
    for x,y,date in zip(df['d18O'].loc[df.Quelle==Q],df.d2H.loc[df.Quelle==Q],df.Datum.loc[df.Quelle==Q]):
        texts.append(plt.text(x,y,str(date)[:10],size=8))

    adjust_text(texts)
    #plt.title('%s iterations' % adjust_text(texts,arrowprops=dict(arrowstyle="-", color='k', lw=0.5),precision=0.001))
    
    ax.set_xlim(ax.get_xlim()[0],ax.get_xlim()[1])

    #gmwl
    x = np.arange(ax.get_xlim()[0]-1,ax.get_xlim()[1]+1,0.1)
    ax.plot(x,x*8+10,label='GMWL')
    ax.fill_between(x,x*8+2,x*8+18,alpha = 0.5,color = "orange")
    
    ax.set(xlabel="$\delta^{18}$O [‰] (VSMOW)",ylabel="$\delta^{2}$H [‰] (VSMOW)")
    ax.tick_params(direction='in',top=True,right=True)
    ax.legend(loc=legpos)
    ax.grid()

    plt.tight_layout()
    plt.show()


def plot_isotopes_dates(size=(16,8),distance=0,legpos=(0.77,0.6)):
    """ Plot the isotope values with dates annotation"""
    plt.rcParams['axes.formatter.use_locale']=True
    df = pd.read_csv('IsotopeData/Isotope_data_Fanes_all.csv',parse_dates=['Datum'])
    fig, ax = plt.subplots(3,1,figsize=cm2inch(size),sharex=True)
    fig.subplots_adjust(hspace=distance)
    Q = df.Quelle.unique()
    ax[0].plot(df['Datum'].loc[df.Quelle==Q[0]],df['d18O'].loc[df.Quelle==Q[0]],'-o',label=Q[0],color='darkslateblue',markersize=3,linewidth=0.5,linestyle='--')
    ax[1].plot(df['Datum'].loc[df.Quelle==Q[1]],df['d18O'].loc[df.Quelle==Q[1]],'-s',label=Q[1],color='darkorange',markersize=3,linewidth=0.5,linestyle='--')
    ax[2].plot(df['Datum'].loc[df.Quelle==Q[2]],df['d18O'].loc[df.Quelle==Q[2]],'-^',label=Q[2][:7],color='tomato',markersize=3,linewidth=0.5,linestyle='--')

    for Quelle,n in zip(Q,range(len(ax))):
        if Quelle != 'LimoseeSüd':
            ax[n].errorbar(df['Datum'].loc[df.Quelle==Quelle],
                        df['d18O'].loc[df.Quelle==Quelle],
                        yerr=df['s.d. d18O'].loc[df.Quelle==Quelle],
                        fmt="none",
                        elinewidth=1,
                        ecolor='black',
                        capsize=2,
                        capthick=0.2)
    for n in range(len(ax)):
        ax[n].xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax[n].xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
        ax[n].grid(alpha=0.2)
        if n == 1:
            ax[n].spines['top'].set_visible(False)
            ax[n].spines['bottom'].set_visible(False)
            ax[n].tick_params(direction='in',top=False,bottom=False,right=True)
            ax[n].set_ylabel(ylabel="$\delta^{18}$O [‰] (VSMOW)",fontsize=10)
        if n == 0:
            ax[n].spines['bottom'].set_visible(False)
            ax[n].tick_params(direction='in',top=True,bottom=False,right=True)
        if n == 2:
            ax[n].spines['top'].set_visible(False)
            ax[n].tick_params(direction='in',top=False,bottom=True,right=True)
        
    fig.autofmt_xdate()
    fig.legend(loc=legpos)
    plt.tight_layout(h_pad=0.4)

def plot_isotopes_dates_single(size=(16,8),distance=0,kwargs_plot={},kwargs_legend={}):
    """ Plot isotopes in a single plot not a subplot"""
    plt.rcParams['axes.formatter.use_locale']=True
    df = pd.read_csv('IsotopeData/Isotope_data_Fanes_all.csv',parse_dates=['Datum'])
    fig, ax = plt.subplots(1,1,figsize=cm2inch(size),sharex=True)
    fig.subplots_adjust(hspace=distance)
    Q = df.Quelle.unique()
    Q = Q.tolist()
    Q.remove('LimoseeSüd')
    ax.plot(df['Datum'].loc[df.Quelle==Q[0]],df['d18O'].loc[df.Quelle==Q[0]],'-o',label=Q[0],color='darkslateblue',**kwargs_plot)
    ax.plot(df['Datum'].loc[df.Quelle==Q[1]],df['d18O'].loc[df.Quelle==Q[1]],'-s',label=Q[1],color='darkorange',**kwargs_plot)
    ax.plot(df['Datum'].loc[df.Quelle==Q[2]],df['d18O'].loc[df.Quelle==Q[2]],'-^',label=Q[2][:7],color='maroon',**kwargs_plot)
    for q in Q:
        ax.errorbar(df['Datum'].loc[df.Quelle==q],
                            df['d18O'].loc[df.Quelle==q],
                            yerr=df['s.d. d18O'].loc[df.Quelle==q],
                            fmt="none",
                            elinewidth=1,
                            ecolor='black',
                            capsize=2,
                            capthick=0.2)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
    ax.yaxis.set_label_text('$\delta^{18}$O [‰, VSMOW]')
    ax.grid(alpha=0.2)

    fig.autofmt_xdate()
    plt.legend(**kwargs_legend)
    plt.tight_layout()
    plt.show()

def cm2inch(*tupl):
    """ Convert cm to inch"""
    inch = 2.54
    if isinstance(tupl[0], tuple):
        return tuple(i/inch for i in tupl[0])
    else:
        return tuple(i/inch for i in tupl)


def plot_snow_height_by_winter(rule='Monthly',kwargs_ros={},kwargs_piz={},legpos=(0.81,0.87),padding=-0.02):
    """ Plot snow heights for each winter in a own subplot"""
    plt.rcParams.update({'font.size':8})
    df_dict = {}
    df_dict['Rossalm'] = dd.ReadfromProcessed('Rossalm',rule,sensor='HS')
    df_dict['Piz La Ila'] = df_piz = dd.ReadfromProcessed('Piz La Ila',rule,sensor='HS')
    df_years_dict={'Rossalm':'','Piz La Ila':''} 
    for k,v in df_dict.items():
        df_dict[k]=v.dropna()
        df_dict[k]=v.set_index('Datum')
        df_years_dict[k] = {'Winter_'+str(year)[-2:]+'_'+str(year+1)[-2:]:df_dict[k][str(year)+'-09':str(year+1)+'-07'] for year in range(1998,2020)}

    for k,v in df_years_dict.items():
        for k_2,v_2 in v.items():
            df_years_dict[k][k_2].loc[pd.isna(df_years_dict[k][k_2].HS)==True]=0

    if rule == 'Monthly':
        nrows,ncols=5,4
        fig, ax = plt.subplots(nrows,ncols,sharey=True,figsize=cm2inch(16,15))
    if rule == 'Daily':
        nrows,ncols=3,2
        fig, ax = plt.subplots(nrows,ncols,sharey=True,figsize=cm2inch(16,15))
    #fig.subplots_adjust(hspace=0,wspace=0)
    keys_ls = [k for k,v in df_dict['Piz La Ila'].items()]
    axes=fig.get_axes()
    del df_years_dict['Piz La Ila']['Winter_01_02'],df_years_dict['Piz La Ila']['Winter_02_03']
    del df_years_dict['Rossalm']['Winter_01_02'],df_years_dict['Rossalm']['Winter_02_03']
    if rule == 'Daily':
        for k in list(df_years_dict.keys()):
            for k_2 in list(v.keys()):
                if df_years_dict[k][k_2].loc[df_years_dict[k][k_2].HS != 0].empty == True:
                    del df_years_dict[k][k_2]
        df_years_dict['Piz La Ila']['Winter_16_17'].loc[(df_years_dict['Piz La Ila']['Winter_16_17'].index >= '2016-10-17')&(df_years_dict['Piz La Ila']['Winter_16_17'].index <='2016-11-03')]=0
    
    for x,k in zip(axes,{**df_years_dict['Rossalm'],**df_years_dict['Piz La Ila']}.items()):
        x.plot(df_years_dict['Rossalm'][k[0]].index,df_years_dict['Rossalm'][k[0]].HS,**kwargs_ros)
        x.plot(df_years_dict['Piz La Ila'][k[0]].index,df_years_dict['Piz La Ila'][k[0]].HS,**kwargs_piz)
        x.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        x.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
        x.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        x.xaxis.set_tick_params(direction='in',which='both',top=True)
        x.yaxis.set_tick_params(direction='in',which='both',right=True,labelright=True)
        x.yaxis.set_major_locator(MultipleLocator(50))
        x.yaxis.set_minor_locator(MultipleLocator(25))
        x.set_ylabel('Schneehöhe [cm]')
        x.annotate(k[0][-5:].replace('_','/'),xy=(0.03,0.89),xycoords='axes fraction')
        handles,labels = x.get_legend_handles_labels()
        x.grid(alpha=0.2)

    remove_internal_ticks(ax,nrows,ncols)
    
    fig.legend(handles,labels,loc=legpos)
    fig.autofmt_xdate()
    fig.tight_layout(h_pad=-0.2,w_pad=0)

    
    return df_years_dict


def remove_internal_ticks(ax,Nrows,Ncols):
    """ Remove ticks inside a subplot"""
    for i in range(Nrows):
        for j in range(Ncols):
            if i == 0:
                ax[i,j].xaxis.set_ticks_position('top')
                plt.setp(ax[i,j].get_xticklabels(), visible=False)
            elif i == Nrows-1:
                ax[i,j].xaxis.set_ticks_position('bottom')
            else:
                ax[i,j].xaxis.set_ticks_position('none')

            if j == 0:
                ax[i,j].yaxis.set_ticks_position('left')
            elif j == Ncols-1:
                ax[i,j].yaxis.set_ticks_position('right')
                ax[i,j].yaxis.set_label_position('right')
                plt.setp(ax[i,j].get_yticklabels(), visible=True)
            else:
                ax[i,j].yaxis.set_ticks_position('none')



def basflow_shed_limo(ET=0.12):
    """ Plot the baseflow compared to precipitation from lake Limo"""
    df = pd.read_csv('LoggerData/Limo_WD_Vol_change.csv',index_col='Datum')
    df.index = pd.to_datetime(df.index)
    df['W_change_shed'] = (df.W_change * 1000)/849285
    df = df['2016-08-13':'2016-12-4']
    df_N = dd.ReadfromProcessed('Wolkenstein','Daily',sensor='N',startdate=str(df.index[0]).replace('-','')[:8],enddate=str(df.index[-1]).replace('-','')[:8])
    df_N['ET_12'] = df_N.N-(df_N.N * ET)
    #df_Epen = cp.calcEvapoArea()
    #df_Epen = df_Epen['2016-08-13':'2016-12-4']
    fig, ax = plt.subplots(figsize=(cm2inch(16,10)))

    ax1 = ax.twinx()

    ax.bar(df_N.Datum,df_N.N,width=1,color='lightblue',label='Niederschlag')
    ax1.bar(df.index,df.W_change_shed,width=1,color='darkorange',label='Seevolumen',alpha=0.7)
    #ax.bar(df_Epen.index, df_Epen.Epen_shed,width=1,color='firebrick',label='See Evaporation')
    ax1.set_ylim(-3,5)
    align_yaxis(ax,ax1)
    ax.set_ylabel('Niederschlag [mm]')
    ax1.set_ylabel('Volumen Änderung [mm]')
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    plt.grid(alpha=0.2)
    fig.legend(bbox_to_anchor=[0.89,0.95])
    fig.autofmt_xdate()
    plt.tight_layout()

    return df


def basflow_shed_parom(ET=0.12):
    """ Plot the baseflow compared to precipitation from lake Limo"""
    df = pd.read_csv('LoggerData/Parom_WD_Vol_change.csv',index_col='Datum')
    df.index = pd.to_datetime(df.index)
    df['W_change_shed'] = (df.W_change * 1000)/8039068
    df = df['2016-06-18':'2016-10-06']
    df_N = dd.ReadfromProcessed('Wolkenstein','Daily',sensor='N',startdate=str(df.index[0]).replace('-','')[:8],enddate=str(df.index[-1]).replace('-','')[:8])
    df_N['ET_12'] = df_N.N-(df_N.N * ET)
    fig, ax = plt.subplots(figsize=(cm2inch(16,10)))

    ax1 = ax.twinx()

    ax.bar(df_N.Datum,df_N.N,width=1,color='lightblue',label='Niederschlag')
    ax1.bar(df.index,df.W_change_shed,width=1,color='darkorange',label='Seevolumen',alpha=0.7)

    align_yaxis(ax,ax1)

    ax.set_ylabel('Niederschlag [mm]')
    ax1.set_ylabel('Volumen Änderung [mm]')
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    plt.grid(alpha=0.2)
    fig.legend(bbox_to_anchor=[0.5,0.96])
    fig.autofmt_xdate()
    plt.tight_layout()

    return df



def align_yaxis(ax1, ax2):
    """ Align yaxis that 0 and 0 is always at the same height """
    y_lims = np.array([ax.get_ylim() for ax in [ax1, ax2]])

    y_lims[:, 0] = y_lims[:, 0].clip(None, 0)
    y_lims[:, 1] = y_lims[:, 1].clip(0, None)
    y_mags = (y_lims[:,1] - y_lims[:,0]).reshape(len(y_lims),1)
    y_lims_normalized = y_lims / y_mags
    y_new_lims_normalized = np.array([np.min(y_lims_normalized), np.max(y_lims_normalized)])
    new_lim1, new_lim2 = y_new_lims_normalized * y_mags
    ax1.set_ylim(new_lim1)
    ax2.set_ylim(new_lim2)



def inflow_outflow():
    """ plot the inflow and outlfow at Lake Limo"""
    limo_file_path = 'LoggerData/Limo_WD_Vol_change.csv'
    df = pd.read_csv(limo_file_path,index_col='Datum')
    df.index = pd.to_datetime(df.index)
    df['W_change_shed'] = (df.W_change * 1000)/849285
    df_N = dd.ReadfromProcessed('Wolkenstein','Daily',sensor='N',startdate=str(df.index[0]).replace('-','')[:8],enddate=str(df.index[-1]).replace('-','')[:8])
    df_snow = cp.SWE_calc_period('Rossalm','St. Martin',startdate=str(df.index[0]).replace('-','')[:8],enddate=str(df.index[-1]).replace('-','')[:8])
    df_snow['snowmelt'] = [df_snow.SWE[n] - df_snow.SWE[n+1] if df_snow.SWE[n+1] < df_snow.SWE[n] else np.nan for n in range(len(df_snow.SWE)-1)]+[np.nan]
    
    fig, ax = plt.subplots(figsize=(cm2inch(16,10)))

    ax1 = ax.twinx()

    ax.bar(df_N.Datum,df_N.N,width=1,color='lightblue',label='Niederschlag')
    ax.bar(df_snow.index, df_snow.snowmelt,width=1,color='green',label='Schneeschmelze')
    ax1.bar(df.index,df.W_change_shed,width=1,color='darkorange',label='Seevolumen',alpha=0.2)
    #ax.bar(df_Epen.index, df_Epen.Epen_shed,width=1,color='firebrick',label='See Evaporation')
    ax1.set_ylim(-3,5)
    align_yaxis(ax,ax1)
    ax.set_ylabel('Niederschlag [mm]')
    ax1.set_ylabel('Volumen Änderung [mm]')
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    plt.grid(alpha=0.2)
    fig.legend(bbox_to_anchor=[0.89,0.95])
    fig.autofmt_xdate()
    plt.tight_layout()

    return df


def ConductivityGaussMixture(data,components,gmm_kwargs={}):
    """ Function which returns the GaussianMixtue of conductivity data
    data: DataFrame (conductivity column named SLT)"""


    plt.rcParams.update({'font.size':8})
    SLT = data.SLT.to_numpy().reshape(-1,1)
    gmm = GaussianMixture(n_components=components,max_iter=1000,**gmm_kwargs).fit(SLT)

    X_plot = np.linspace(100,round(SLT.max()+51,-2), 10000)[:, np.newaxis]

    gmm_mean = gmm.means_.flatten()
    gmm_weight = gmm.weights_.flatten()
    gmm_covari = np.sqrt(gmm.covariances_).flatten()

    kde = KernelDensity(kernel="gaussian", bandwidth=5).fit(SLT)
    log_dens = kde.score_samples(X_plot)

    fig,ax = plt.subplots(figsize=(cm2inch(16,10)))

    ax.plot(X_plot[:,0],np.exp(log_dens), color='slateblue',label='Häufigkeitsverteilung',linestyle='--')

    loop_ls = [np.where(gmm.weights_.flatten()== -np.sort(-gmm.weights_.flatten())[n])[0][0] for n in range(0,components)]


    for count,n in enumerate(loop_ls):
        normstats = stats.norm.pdf(X_plot, gmm_mean[n] ,gmm_covari[n])*gmm_weight[n]
        ax.plot(X_plot[:,0],normstats,zorder=2, label='P{} bei {} [\u03BCS/cm]'.format(count+1,str(round(gmm_mean[n],1))).replace('.',','))
        ax.annotate('P{}'.format(count+1),xy=(gmm_mean[n],normstats.max()+0.0005),xycoords='data',ha='center',va='bottom',zorder=5,
                        path_effects=[PathEffects.withStroke(linewidth=3,foreground="w")])


    ax.legend(loc='upper right')
    ax.set_xlabel('Leitfähigkeit [\u03BCS/cm]')
    ax.set_ylabel('Häufigkeit')

    return gmm_mean,gmm_weight, gmm_covari




def Plot_Lavarella_Ros_Wolk(df='',left='',right='',rule='',
                            xmargin=0.07,height=-0.8,minter=2,legpos=(0.68,0.86),
                            n_station='Wolkenstein',roll=24,slt_lim=(100,250),
                            temp_lim=(3,4),sensor_ros='HS',clean_mes=False):
    """ Plot spring water data with snowheight and precip"""
    plt.rcParams.update({'font.size':8})
    if rule == '':
        rule = 'Daily'
    if df == '':
        df = LoadLavarellaQuelleExcel(rule=rule)

    df_N = dd.ReadfromProcessed(n_station,rule,sensor='N',startdate=str(df.index[0]).replace('-','')[0:8],enddate=str(df.index[-1]).replace('-','')[0:8])
    df_HS = dd.ReadfromProcessed('Rossalm',rule,startdate=str(df.index[0]).replace('-','')[0:8],enddate=str(df.index[-1]).replace('-','')[0:8])
    if left == '' and right!='':
        left = df.index[0]
    if left != '' and right == '':
        right = df.index[-1]
    if left == '' and right == '':
        right = df.index[-1]
        left = df.index[0]
    if clean_mes == True:
        df.loc[((df['SLT']-df['SLT'].shift())>5),'WT']=np.nan
        df.loc[((df['SLT']-df['SLT'].shift())<-5),'WT']=np.nan
        df.loc[(df['SLT']<=50),'WT']=np.nan
        df.loc[((df['SLT']-df['SLT'].shift())>5),'SLT']=np.nan
        df.loc[((df['SLT']-df['SLT'].shift())<-5),'SLT']=np.nan
        df.loc[(df['SLT']<=50),'SLT']=np.nan


    if rule == "Hourly":
        df['SLT_rol'] = df.SLT.rolling(roll,center=True,min_periods=int(roll/1.5)).mean()
        df['WT_rol'] = df.WT.rolling(roll,center=True,min_periods=int(roll/1.5)).mean()
        df_HS['HS_rol'] = df_HS.HS.rolling(roll,center=True,min_periods=int(roll/1.5)).mean()
        df_HS['LT_rol'] = df_HS.LT.rolling(roll,center=True,min_periods=int(roll/1.5)).mean()
    
        

    df = df.loc[(df.index>left)&(df.index<right)]
    df_N = df_N.loc[(df_N.Datum>left)&(df_N.Datum<right)]
    df_HS = df_HS.loc[(df_HS.Datum>left)&(df_HS.Datum<right)]

  

    fig, ax = plt.subplots(2,1,figsize=cm2inch(16,12),sharex=True)
    fig.subplots_adjust(hspace=0)

    ax3 = ax[0]
    ax =  ax[1]
    ax1 = ax.twinx()
    ax2 = ax.twinx()
    if len(sensor_ros.split(',')) > 1:
        ax4 = ax3.twinx()

    if rule != "Hourly":
        ax.plot(df.index, df.WT,c='slateblue',label='WT')
        ax1.plot(df.index,df.SLT,c='firebrick',label='SLT',alpha=0.8)
        if ('HS' in sensor_ros) & ('LT' in sensor_ros):
            ax3.plot(df_HS.Datum,df_HS.HS,c='orange',label='HS')
            ax4.plot(df_HS.Datum,df_HS.LT,c='darkblue',label='LT')
        else:
            if 'HS' in sensor_ros:
                ax3.plot(df_HS.Datum,df_HS.HS,c='orange',label='HS')
            if 'LT' in sensor_ros:
                ax3.plot(df_HS.Datum,df_HS.LT,c='darkblue',label='LT')
    
    ax2.bar(df_N.Datum,df_N.N,width=width_bar_plot(rule,df_N),align='edge',color='grey',label='N')
    
    if rule == "Hourly":
        ax.plot(df.index, df.WT,c='slateblue',label='WT',alpha=0.6)
        ax1.plot(df.index,df.SLT,c='firebrick',label='SLT',alpha=0.6)
        ax1.plot(df.index,df['SLT_rol'],c='firebrick')
        ax.plot(df.index,df['WT_rol'],c='slateblue')
        if ('HS' in sensor_ros) & ('LT' in sensor_ros):
            ax3.plot(df_HS.Datum,df_HS.HS,c='orange',label='HS',alpha=0.6)
            ax3.plot(df_HS.Datum,df_HS.HS_rol,c='orange')
            ax4.plot(df_HS.Datum,df_HS.LT,c='darkblue',label='LT',alpha=0.6)
            ax4.plot(df_HS.Datum,df_HS.LT,c='darkblue')
        else:
            if 'HS' in sensor_ros:
                ax3.plot(df_HS.Datum,df_HS.HS,c='orange',label='HS',alpha=0.6)
                ax3.plot(df_HS.Datum,df_HS.HS_rol,c='orange')
            if 'LT' in sensor_ros:
                ax3.plot(df_HS.Datum,df_HS.LT,c='darkblue',label='LT',alpha=0.6)
                ax3.plot(df_HS.Datum,df_HS.LT,c='darkblue')

    set_xmargin(ax2,left=0,right=xmargin)
    set_xmargin(ax,left=0,right=xmargin)
    

    if rule != "Hourly":
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=minter))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b'%y"))
    else:
        ax2.xaxis.set_major_locator(mdates.HourLocator(interval=minter))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))
        ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=minter//2))

    for axes in fig.axes:
        if axes.bbox.bounds[1] > ax1.bbox.bounds[1]:
            axes.spines['bottom'].set_visible(False)
            axes.xaxis.set_tick_params(direction='in',bottom=False,top=True)
        else:
            axes.spines['top'].set_visible(False)
            

    ax1.yaxis.set_label_text('Leitfähigkeit [\u03BCS/cm]')
    ax1.yaxis.set_label_coords(x=0.92,y=0.5)
    ax.yaxis.set_label_text('Wassertemperatur [°C]')
    #ax.yaxis.set_label_coords(x=1.1,y=0.5)
    ax.yaxis.set_label_position('right')
    ax2.yaxis.set_label_text('Niederschlag [mm]')
    ax2.yaxis.set_label_position('left')
    #ax2.yaxis.set_label_coords(x=-0.05,y=0.68)
    if ('HS' in sensor_ros) & ('LT' in sensor_ros):
        ax3.yaxis.set_label_text('Schneehöhe [cm]')
        ax3.yaxis.set_label_position('left')
        ax4.yaxis.set_label_text('Lufttemperatur [°C]')
        ax4.yaxis.set_label_position('right')
    else:
        if 'HS' in sensor_ros:
            ax3.yaxis.set_label_text('Schneehöhe [cm]')
            ax3.yaxis.set_label_position('left')
        if 'LT' in sensor_ros:
            ax3.yaxis.set_label_text('Lufttemperatur [°C]')
            ax3.yaxis.set_label_position('left')

    ax.yaxis.set_tick_params(direction='out',left=False,right=True,labelleft=False,labelright=True)
    ax2.yaxis.set_tick_params(direction='out',left=True,right=False,labelleft=True,labelright=False)
    ax1.yaxis.set_tick_params(direction='in',pad=-5)

    ax1.set_ylim(slt_lim)
    ax.set_ylim(temp_lim)

    ax.set_yticklabels(['']+[str(round(x,2)).replace('.',',') for x in ax.get_yticks().tolist()][1:])
    ax1.set_yticklabels(['']+[int(x) for x in ax1.get_yticks().tolist()[1:]] ,ha='right')

    hand = [axes.get_legend_handles_labels()[0][0] for axes in fig.axes]
    lab = [axes.get_legend_handles_labels()[1][0] for axes in fig.axes]
    ax3.legend(handles=hand,labels=lab,loc='best')

    fig.autofmt_xdate()
    fig.align_ylabels()

    
def linreg_snow_lake(x,y,linnum=1000,ylabel='akkumulierter SWE [mm]',xlabel='Volumen [m³]',ax=None):
    """ Plot a linear regression between snow and lakevolume"""
    if ax is None:
        fig,ax = plt.subplots(figsize=cm2inch(8,8))

    model = np.polyfit(x,y,1)
    predict = np.poly1d(model)
    x_new = np.linspace(0,x.max()+(x.max()/4),linnum)
    ax.scatter(x,y,marker='x',color='xkcd:burnt orange')
    ax.plot(x_new,predict(x_new),color='xkcd:indigo')
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.text(0.01,0.99,'r²={:.2f}'.format(r2_score(y, predict(x))).replace('.',','),transform=ax.transAxes,va='top')
    plt.tight_layout()


def Vol_Snow_Linreg(snow=True,SWE=False,lake='Limo'):
    """ Prepare the data to plot a linear regression between maximum volume and max snow accumulation or max SWE accumulation"""
    if (snow==True) & (SWE==True):
        print('Decide between volume and SWE')
        return
    if lake== 'Limo':
        df_vol = pd.read_csv('LoggerData/Limo_calcs_measurements_new.csv')
        df_vol.index = pd.to_datetime(df_vol.Datum)
    else:
        df_vol = pd.read_csv('LoggerData/Parom_calcs_measurements_new.csv')
        df_vol.index = pd.to_datetime(df_vol.Datum)



    vol_dict = {str(year):{'data':df_vol[str(year)]} for n,year in enumerate(df_vol.index.year.unique()) if year < 2020}
    for k,v in vol_dict.items():
        v['max_log'] = v['data'].Volume.max()
        v['max_calc']= v['data'].Vol_calc.max()
    df_vol_tot = pd.DataFrame().from_dict(vol_dict,orient='index')
    df_vol_tot['max_vol'] = [data if pd.isna(data)==False else df_vol_tot.max_calc[n] for n,data in enumerate(df_vol_tot.max_log)]
    
    swe = cp.SWE_calc_period('Rossalm','Wolkenstein','20141001',"20200601")

    if SWE == True:
        swe_dict = {str(year) +'-'+ str(year+1):swe[str(year)+'-10':str(year+1)+'-06'] for n,year in enumerate(swe.index.year.unique()) if year < 2020}
        for k,v in swe_dict.items():
            swe_dict[k]['SWE_sum'] = [0]+[v.SWE[n+1] - v.SWE[n] if  (v.SWE[n+1] > v.SWE[n]) & (v.SWE[n+1] - v.SWE[n] > 0 ) else 0 for n in range(len(v.SWE)-1)]
            swe_dict[k]['SWE_cumsum'] = swe_dict[k]['SWE_sum'].cumsum()
        max_swe_sum = {k:v.SWE_cumsum.max() for k,v in swe_dict.items()}
        df_swe = pd.DataFrame().from_dict(max_swe_sum,orient='index',columns=['SWE_max'])


    if snow == True:
        swe_dict = {str(year) +'-'+ str(year+1):swe[str(year)+'-10':str(year+1)+'-06'] for n,year in enumerate(swe.index.year.unique()) if year < 2020}
        for k,v in swe_dict.items():
            swe_dict[k]['SWE_sum'] = [0]+[v.HS[n+1] - v.HS[n] if  (v.HS[n+1] > v.HS[n]) & (v.HS[n+1] - v.HS[n] > 0 ) else 0 for n in range(len(v.HS)-1)]
            swe_dict[k]['SWE_cumsum'] = swe_dict[k]['SWE_sum'].cumsum()
        max_swe_sum = {k:v.SWE_cumsum.max() for k,v in swe_dict.items()}
        df_swe = pd.DataFrame().from_dict(max_swe_sum,orient='index',columns=['SWE_max'])

    linreg_snow_lake(df_vol_tot.max_vol,df_swe.SWE_max[:-1])

