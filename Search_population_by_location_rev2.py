from h3 import h3
from Database_Population import *
from datetime import datetime, date, timedelta
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from csv_join_tambon import Reverse_GeoCoding
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob
import pyodbc
import warnings

warnings.filterwarnings('ignore')

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

# Read population data
def Read_FB_Population_DB(province):
    print('------------- Start ReadDB -------------')
    ## ODBC Driver 17 for SQL Server
	
    # SQL Server
    conn1 = connect_tad

    cursor = conn1.cursor()
    #- Select data  all records from the table
    sql="""

     SELECT  [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
      ,[population_under_five]
      ,[population_515_2560]
      ,[population_men]
      ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
    FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]
    where [p_name_t]>= '"""+str(province)+"""' 
    
    """    
    dfout=pd.read_sql(sql,conn1)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)    
    del conn1, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def GetPopulationDensity(x,dfIn):
    dfDummy=dfIn[dfIn['hex_id']==x].copy().reset_index(drop=True)
    population=0
    if(len(dfDummy)>0):
        population=dfDummy['population'].values[0]
        #print(population, ' ----  ',type(population))
    del dfDummy
    return population

########################################################################################################
######  Input ----  ####################################################################################
######  Goal: To find population of the grid on which the store is located

# level 8 covers approx 1 km2
h3_level=8   

# Read store location data
filename='รายชื่อร้านค้าดวงดาวทั้งหมด (59,961 ร้านค้า).xlsx'


data_path='C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Find_Population_by_location\\'

#######################################################################################################
cvt={'CustomerCode':str,'CustomerType':str,'EmployeeId':str,'ประเภทร้านค้า':str}
dfIn=pd.read_excel(data_path+filename,sheet_name='Sheet1',converters=cvt, engine='openpyxl')
originalLen=len(dfIn)
print(len(dfIn), ' ----  ',dfIn.head(10))

print(' ===>  Reverse Geocoding')
includeList=['CustomerCode','CustomerName','CustomerAddress','CustomerType','Latitude','Longitude']
dfIn=Reverse_GeoCoding(dfIn[includeList])  # beware dropped rows which csv_join_tambon could not find the lat and lng

geocodeLen=len(dfIn)

print('********* compare :  O ',originalLen,' ---> G ',geocodeLen)  # all can identify province now : 886012418 276007876 307001252 206002806

print(len(dfIn), ' - after---  ',dfIn.head(10), ' ::  ',dfIn.columns)
dfIn.to_csv(data_path+'geocoded.csv')

includeList=['CustomerCode', 'CustomerName', 'CustomerAddress', 'CustomerType',
       'Latitude', 'Longitude',  'p_name_t', 'a_name_t',  't_name_t','s_region']
dfIn_2=dfIn[includeList].copy().reset_index(drop=True)
print(len(dfIn_2), ' - filtered ---  ',dfIn_2.head(10), ' ::  ',dfIn_2.columns)
del dfIn

provinceList=list(dfIn_2['p_name_t'].unique())
print(' :: ',len(provinceList), ' :: ')

mainDf=pd.DataFrame(columns=['CustomerCode', 'CustomerName', 'CustomerAddress', 'CustomerType',
       'Latitude', 'Longitude', 'p_name_t', 'a_name_t', 't_name_t',
       's_region', 'hex_id', 'population'])

checkList=[]     

#provinceList=['นครศรีธรรมราช']    #'ภูเก็ต',
for province in provinceList:  #[:2]:
    print(' ===> ',province)
    ### Select store location data by provicne
    dfDummy=dfIn_2[dfIn_2['p_name_t']==province].copy().reset_index(drop=True)
    print(len(dfDummy), ' -- dummy ---  ',dfDummy.head(10))
    originalLen=len(dfDummy)
    

    #### compute hex_id of each store location : Latitude and Longitude
    dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)  
    dfDummy.to_csv(data_path+province+'_hex_id.csv')
    midLen=len(dfDummy)

    #### read population data by hexagonal grids stored on the database
    dfProvince=Read_FB_Population_DB(province)
    print(len(dfProvince), ' -- province ---  ',dfProvince.head(10),' ------  ',dfProvince.columns)
    dfProvince.to_csv(data_path+province+'_province.csv')

    #### merge store location data and the retrieved population data at associated grids
    #subDf=pd.merge(dfDummy, dfProvince, how="left", on=['hex_id'])
    #subDf=dfDummy.merge(dfProvince, how="left", on='hex_id')
    dfDummy['population']=dfDummy.apply(lambda x:GetPopulationDensity(x['hex_id'], dfProvince),axis=1)   # Need to assign row by row because merging results in extra rows
    subDf=dfDummy.copy()   
    
    subDf.to_csv(data_path+province+'_subDf.csv')
    afterLen=len(subDf)
    print(len(subDf), ' -- subDf ---  ',subDf.head(10),' ------  ',subDf.columns)
    includeList=['CustomerCode', 'CustomerName', 'CustomerAddress', 'CustomerType',
       'Latitude', 'Longitude', 'p_name_t', 'a_name_t', 't_name_t',
       's_region', 'hex_id', 'population']
    subDf_1=subDf[includeList]
    #subDf_1.rename(columns={'Latitude_x':'Latitude','Longitude_x':'Longitude','p_name_t_x':'p_name_t'}, inplace=True)
    subDf_1['CustomerCode']=subDf_1['CustomerCode'].astype(str)
    #subDf_1.to_csv(data_path+province+'_population_by_province.csv')
    mainDf=mainDf.append(subDf_1).reset_index(drop=True)
    print('****************'+str(province)+' -- '+str(originalLen)+' --  '+str(midLen)+ ' -- '+str(afterLen)+'***********')
    #checkList.append(str(province)+' -- '+str(originalLen)+' --  '+str(midLen)+ ' -- '+str(afterLen))
    del subDf, subDf_1

print(len(mainDf), ' -- mainDf ---  ',mainDf.head(10),' ------  ',mainDf.columns)
mainDf.to_csv(data_path+'maindf_population_by_province_PAT.csv')
print(' cl : ',checkList)
#clDf=pd.DataFrame(checkList, columns=['checkList'])
#clDf.to_csv(data_path+'checkList.csv')
del dfDummy, mainDf, dfIn_2
del includeList, provinceList
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')