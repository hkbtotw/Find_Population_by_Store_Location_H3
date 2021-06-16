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
import warnings
import pyodbc

warnings.filterwarnings('ignore')

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

def Read_FB_Population_Dictinct_Prv():
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql="""SELECT distinct p_name_t FROM public.\"fb_population_general\" """


        dfout = pd.read_sql_query(sql, connection)

        print(' ==> ',dfout)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def ReadProvince():
    print('------------- Start ReadDB -------------')

    ## ODBC Driver 17 for SQL Server
    # conn = pyodbc.connect('Driver={SQL Server};'
    #                         'Server=SBNDCBIPBST02;'
    #                         'Database=SR_APP;'
    #                     'Trusted_Connection=yes;')

    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST02;'
                            'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    #- Select data  all records from the table
    sql="""

     select distinct PROVINCE_TH from [TSR_ADHOC].[dbo].[DIM_TH_R_PROVINCE]
  where REGION_TBL<>'-'

    """
    
    dfout=pd.read_sql(sql,conn)
    
    print(len(dfout.columns),' :: ',dfout.columns)
    print(dfout)
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout


provinceDf=Read_FB_Population_Dictinct_Prv()
print(' ---> ',provinceDf)
provinceList=list(provinceDf['p_name_t'].unique())

mainProvince=ReadProvince()
print(' --> ',mainProvince)
mainList=list(mainProvince['PROVINCE_TH'].unique())

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3



resultList=intersection(mainList, provinceList)
print(len(mainList),' ===> ',len(provinceList))
print(' ==> ', len(resultList))

main_list = [item for item in mainList if item not in resultList]
print(' ==> ', len(main_list))

mainDf=pd.DataFrame(main_list, columns=['list'])
mainDf.to_csv('C:\\Users\\70018928\\Documents\\Project2021\\Experiment\\Find_Population_by_location\\'+'province_not_in_facebookdata.csv')