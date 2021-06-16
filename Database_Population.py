from typing import no_type_check_decorator
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from Credential import *
from math import radians, cos, sin, asin, sqrt
import numpy as np
import psycopg2
import pyodbc

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Food\\FB_Population\\'
file_name='test_data.xlsx'


def Read_FB_Population_women(prv_input):
        print('------------- Start ReadDB - fb_population_women -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_women\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_women\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout



def Read_FB_Population_men(prv_input):
        print('------------- Start ReadDB - fb_population_men -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_men\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_men\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout


def Read_FB_Population_children_under_five(prv_input):
        print('------------- Start ReadDB - fb_population_elderly_60_plus -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_children_under_five\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_children_under_five\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Read_FB_Population_elderly_60_plus(prv_input):
        print('------------- Start ReadDB - fb_population_elderly_60_plus -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_elderly_60_plus\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_elderly_60_plus\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Read_FB_Population_Youth_15_24_Prv(prv_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_youth_15_24\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_youth_15_24\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

def Read_FB_Population_General_Prv(prv_input, d_input, s_input):
        print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""

        ## Not working coz in sub district level one will only get information within 5 km, beyond that will need data across district
        # if(len(s_input)>0):    
        #         print(' Sub district **************************************** ')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where t_name_t = '"""+str(s_input)+"""'  """
        # elif(len(d_input)>0):                
        #         print(' District +++++++++++++++++++++++++++++++++++++++++++++++')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where a_name_t = '"""+str(d_input)+"""'  """
        # elif(len(prv_input)>0):
        #         print(' Provicne ------------------------------------------------- ')            
        #         sql = """SELECT * FROM public.\"fb_population_general\" where p_name_t = '"""+str(prv_input)+"""'  """
        # else:
        #         print( ' ALL =================================================== ')
        #         sql = """SELECT * FROM public.\"fb_population_general\" """

        if(len(prv_input)>0):
                print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"fb_population_general\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"fb_population_general\" """

        dfout = pd.read_sql_query(sql, connection)

        print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                print("PostgreSQL connection is closed")    

        return dfout

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

def Write_FB_Population_Clustered(df_input):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    conn1 = pyodbc.connect('Driver={SQL Server};'
                            'Server=SBNDCBIPBST02;'
                            'Database=TSR_ADHOC;'
                        'Trusted_Connection=yes;')
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[FB_Population_Clustered]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[FB_Population_Clustered]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[FB_Population_Clustered] (	

      [Latitude]
      ,[Longitude]
      ,[total_population]
      ,[cluster_number]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?
  
    )""", 
      row['Latitude']
      ,row['Longitude']
      ,row['total_population']
      ,row['cluster_number']
      ,row['p_name_t']
      ,row['DBCreatedAt']
     ) 
    conn1.commit()

    cursor.close()
    conn1.close()
    print('------------Complete WriteDB-------------')

#### Run this script to get distinct province names
#distinctPrv=Read_FB_Population_Dictinct_Prv()
#distinctPrv.to_csv(file_path+'prv_distinct.csv')
#print(' ===> ',distinctPrv)

#prvList=['ฉะเชิงเทรา','ระยอง','ชลบุรี','กรุงเทพมหานคร','ปทุมธานี']

def Read_Location_Population(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_General_Prv(prv_name, '', '')
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

def Read_Location_Population_Youth_15_24(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_Youth_15_24_Prv(prv_name)
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

def Read_Location_Population_elderly_60_plus(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_elderly_60_plus(prv_name)
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

def Read_Location_Population_children_under_five(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_children_under_five(prv_name)
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

def Read_Location_Population_men(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_men(prv_name)
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

def Read_Location_Population_women(province):

    prvList=[province]

    for prv_name in prvList:  #[:2]:
        print(' ===> ',prv_name)
        dfIn=Read_FB_Population_women(prv_name)
        dfIn.rename(columns={'lng':'Longitude','lat':'Latitude'}, inplace=True)
        print(len(dfIn), ' --------  in ------ ',dfIn.head(10))
        #dfIn.to_csv(file_path+'pathumthani.csv')

    del prvList

    return dfIn

###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')