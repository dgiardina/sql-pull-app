from flask import Flask
import dash
from dash import html, Dash, dcc, Input, Output, callback

import pandas as pd
from pandas import DataFrame
import numpy as np
import datetime as dt
from datetime import datetime, timedelta


import urllib.request
import urllib.error
import psycopg2
from psycopg2 import Error
import sqlalchemy
from sqlalchemy import create_engine, text



def url_gen_func(logger_name, start_date,end_date):

    index = pd.date_range(start = start_date, end=end_date) # generate a dt array from start_date n days back
    df = pd.DataFrame(index=index)
    
    df['log_name'] = logger_name
    df['date_vec'] = df.index.date
    df['date_vec'] = df['date_vec'].astype(str) # convert to string for url gen
    df['date_vec'] = df['date_vec'].apply(lambda x: np.char.replace(x, "-", "/")) 
    df['url'] = "https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw/hawaii/" +  df['date_vec'] +"/" + df['log_name']+"_MetData.dat"
    return df['url']

def data_cat_func(url_vec,col):
    url_vec = url_vec.iloc[::-1] # invert url vec so that dfs concatonate properly 

    def durl_pull(url,col):
        try:

            df = pd.read_table(url,
                                skiprows=2,
                                sep=',',
                                names=col # replace headers with standard names
                                )
            
            df = df.iloc[2:,:] # remove extra rows and reset index
            df = df.reset_index(drop = True) 
            print('Success!')
            return df

        except urllib.error.HTTPError as err:
            
            print(f'A HTTPError was thrown: {err.code} {err.reason}')
            df = pd.DataFrame()
            return df
    
    # df_cat= durl_pull(url_vec[0],col)
    df_cat= pd.DataFrame()

    for i in url_vec:
        try:
        
            df = durl_pull(i,col) 
            df = df.iloc[::-1] # reinvert url to concatonate
            df_cat = pd.concat([df_cat,df]) #concat dfs
            # print("getting data")
        except Exception as e:
            print("skipping url do to an exception",e)
    # return df_cat.iloc[::-1]
    return df_cat

def convert_datetime(datetime_str):
    datetime_format = "%Y-%m-%d %H:%M:%S"
    next_day = timedelta(days=1)

    if datetime_str.endswith(" 24:00:00"):
        datetime_str = datetime_str.replace(" 24:00:00", " 00:00:00")
        datetime_obj = datetime.strptime(datetime_str, datetime_format)
        datetime_obj += next_day
        return datetime_obj
    else:
        return datetime.strptime(datetime_str, datetime_format)

def to_df_func(sta_id, sql_id, staname, start_date,end_date):
    url_vec = url_gen_func(staname, start_date,end_date)
    if sta_id == '0154':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'Tair_2_Avg', 
            'RH_1_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VP_2_Avg', 'VPsat_1_Avg', 'VPsat_2_Avg', 'VPD_1_Avg', 'VPD_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 'WD_StdCS', 'P', 'Psl', 'Tsoil_d1_Avg', 'SHF_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg', 'Tsoil_d2', 'Tsoil_d3', 'Tsoil_d4', 'RF_Tot', 'RGtip_Tot', 'RFint_Max']
    elif sta_id == '0521':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'Tair_2_Avg', 
            'RH_1_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VP_2_Avg', 'VPsat_1_Avg', 'VPsat_2_Avg', 'VPD_1_Avg', 'VPD_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 'WD_StdCS', 'P', 'Psl', 'Tsoil_d1_Avg', 'SHF_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg', 'Tsoil_d2', 'Tsoil_d3', 'Tsoil_d4', 'RF_Tot', 'RGtip_Tot', 
            'RFint_Max', 'FC_Tot','WTlvl_Avg']
    elif sta_id == '0602':
        col=['TIMESTAMP','RECORD','Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VPsat_1_Avg', 'VP_2_Avg', 
            'VPsat_2_Avg', 'WS_Avg', 'WDuv_Avg', 'WD_StdY', 'WS_Avg2', 'WDuv_Avg2', 'WD_StdY2', 'SWin_Avg', 'SWout_Avg', 'LWin_Avg', 
            'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'P', 'Psl', 'RF_Tot', 'RFint_Max', 
            'SM_d1_Avg', 'Tsoil_d2', 'SM_d2_Avg', 'Tsoil_d3', 'SM_d3_Avg', 'Tsoil_d4', 'SHFsrf_Avg', 'Tsoil_d1_Avg']
    elif sta_id == '0119':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'Trnet_Avg', 'Rnet_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 'TCair_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_d1_Avg', 'SHF_Avg', 'SHF_2_Avg', 'SHF_Avg2', 'SHS_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SMadjT_d1_Avg', 'SMadjT_d2_Avg', 'RF_Tot']
    elif sta_id == '0152':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'Trnet_Avg', 'Rnet_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 'TCair_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_d1_Avg', 'SHF_Avg', 'SHF_2_Avg', 'SHF_Avg2', 'SHS_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SMadjT_d1_Avg', 'SMadjT_d2_Avg', 'RF_Tot']
    elif sta_id == '0153':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'Trnet_Avg', 'Rnet_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 'TCair_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_d1_Avg', 'SHF_Avg', 'SHF_2_Avg', 'SHF_Avg2', 'SHS_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SMadjT_d1_Avg', 'SMadjT_d2_Avg', 'RF_Tot']
    elif sta_id == '0151':
        col=['TIMESTAMP','RECORD',"SWin_Avg", "SWout_Avg", "LWin_Avg", "LWout_Avg", "Trnet_Avg", "Rnet_Avg", "SWnet_Avg", "LWnet_Avg", 
            "Rin_Avg", "Rout_Avg", "Albedo_Avg", "Tsrf_Avg", "Tsky_Avg", "Tair_1_Avg", "RH_1_Avg", "Tair_2_Avg", "RH_2_Avg", "TCair_Avg", 
            "WS_Avg", "WSrs_Avg", "WDrs_Avg", "WD_StdCS", "WDuv_Avg", "Tsoil_d1_Avg", "SHF_Avg", "SHF_2_Avg", "SHF_Avg2", "SHS_Avg", 
            "SHFsrf_Avg", "SM_d1_Avg", "SM_d2_Avg", "SMadjT_d1_Avg", "SMadjT_d2_Avg", "RF_Tot", "FOGtip_Tot"]
    elif sta_id == '0141':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWinUC_Avg', 'LWoutUC_Avg', 'Trnet_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 
            'RH_2_Avg', 'TC_air_Avg', 'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_TCAV_Avg', 
            'SHF_Avg', 'SHF_2_Avg', 'SM_d2_Avg', 'SM_d1_Avg', 'SMadjT_d2_Avg', 'SMadjT_d1_Avg', 'RF_Tot']
    elif sta_id == '0143':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'Tair_2_Avg', 
            'RH_1_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VP_2_Avg', 'VPsat_1_Avg', 'VPsat_2_Avg', 'VPD_1_Avg', 'VPD_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 'WD_StdCS', 'P', 'Psl', 'Tsoil_d1_Avg', 'SHF_Avg', 'SHF_2_Avg', 
            'SM_d1_Avg', 'SMadjT_d1_Avg', 'RF_Tot', 'FOGtip_Tot']
    elif sta_id == '0501':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWinUC_Avg', 'LWoutUC_Avg', 'Trnet_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg',
            'RH_2_Avg', 'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_d1_Avg', 'SHF_Avg', 'SHF_2_Avg', 'SM_d1_Avg', 
            'SM_d2_Avg', 'SM_d3_Avg', 'SMus_d1_Avg', 'SMus_d2_Avg', 'SMus_d3_Avg', 'SMadjT_d1_Avg', 'RF_Tot']
    elif sta_id == '0281':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'RnetUC_Avg', 'Rnet_Avg', 'PAR_Avg', 'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'SM_d1_Avg', 'RF_Tot']
    elif sta_id == '0282':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 'Albedo_Avg', 'Rnet_Avg',
            'Tair_1_Avg', 'RH_1_Avg', 'Tair_2_Avg', 'RH_2_Avg', 'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'Tsoil_d1_Avg', 
            'SHF_Avg', 'SHF_2_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg', 'RF_Tot']

    elif sta_id == '0283':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWinUC_Avg', 'LWoutUC_Avg', 'Trnet_Avg', 'PAR_Avg', 'Tair_1_Avg', 'RH_1_Avg', 
            'Tair_2_Avg', 'RH_2_Avg', 'PAR_2_Avg', 'PARdiff_Avg', 'WS_Avg', 'WSrs_Avg', 'WDrs_Avg', 'WD_StdCS', 'WDuv_Avg', 'RF_Tot', 
            'Tsoil_d1_1_Avg', 'Tsoil_d1_2_Avg', 'SHF_Avg', 'SHF_2_Avg', 'SHF_3_Avg', 'SHF_4_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg']

    elif sta_id == '0286':
        col=['TIMESTAMP','RECORD',"SWin_Avg", "SWout_Avg", "LWinUC_Avg", "LWoutUC_Avg", "Trnet_Avg", "PAR_Avg", "Tair_1_Avg", "RH_1_Avg", 
            "Tair_2_Avg", "RH_2_Avg", "WS_Avg", "WSrs_Avg", "WDrs_Avg", "WD_StdCS", "WDuv_Avg", "SM_d1_Avg", "RF_Tot"]

    elif sta_id == '0287':
        col=['TIMESTAMP','RECORD',"SWin_Avg", "SWout_Avg", "LWin_Avg", "LWout_Avg", "SWnet_Avg", "LWnet_Avg", "Albedo_Avg", "Rnet_Avg", 
            "Tair_1_Avg", "RH_1_Avg", "Tair_2_Avg", "RH_2_Avg", "WS_Avg", "WSrs_Avg", "WDrs_Avg", "WD_StdCS", "WDuv_Avg", "SM_d1_Avg", 
            "SM_d2_Avg", "RF_Tot"]
    elif sta_id == '0288':
        col=['TIMESTAMP','RECORD',"SWin_Avg", "SWout_Avg", "LWinUC_Avg", "LWoutUC_Avg", "Trnet_Avg", "Tair_1_Avg", "RH_1_Avg", 
            "Tair_2_Avg", "RH_2_Avg", "WS_Avg", "WSrs_Avg", "WDrs_Avg", "WD_StdCS", "WDuv_Avg", "RF_Tot", "Tsoil_d1_1_Avg", 
            "Tsoil_d1_2_Avg", "SHF_Avg", "SHF_2_Avg", "SHF_3_Avg", "SHF_4_Avg", "SM_d1_Avg", "SM_d2_Avg", "SM_d3_Avg"]

    elif sta_id == '0502':
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'Tair_2_Avg', 
            'RH_1_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VP_2_Avg', 'VPsat_1_Avg', 'VPsat_2_Avg', 'VPD_1_Avg', 'VPD_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 'WD_StdCS', 'P', 'Psl', 'Tsoil_d1_Avg', 'SHF_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg', 'Tsoil_d2', 'Tsoil_d3', 'Tsoil_d4', 'RF_Tot', 'RGtip_Tot', 'RFint_Max']
        
    else:
        col=['TIMESTAMP','RECORD','SWin_Avg', 'SWout_Avg', 'LWin_Avg', 'LWout_Avg', 'SWnet_Avg', 'LWnet_Avg', 
            'Rin_Avg', 'Rout_Avg', 'Rnet_Avg', 'Albedo_Avg', 'Tsrf_Avg', 'Tsky_Avg', 'Tair_1_Avg', 'Tair_2_Avg', 
            'RH_1_Avg', 'RH_2_Avg', 'VP_1_Avg', 'VP_2_Avg', 'VPsat_1_Avg', 'VPsat_2_Avg', 'VPD_1_Avg', 'VPD_2_Avg', 
            'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 'WD_StdCS', 'P', 'Psl', 'Tsoil_d1_Avg', 'SHF_Avg', 
            'SHFsrf_Avg', 'SM_d1_Avg', 'SM_d2_Avg', 'SM_d3_Avg', 'Tsoil_d2', 'Tsoil_d3', 'Tsoil_d4', 'RF_Tot', 'RGtip_Tot', 'RFint_Max']

    df = data_cat_func(url_vec,col)
    # print(df)
    return df

def to_sql_func(df, engine,if_exist):
    sql_id = df.iloc[0]['sql_id']
    print(sql_id)
    dtype = {
        "timestamp": sqlalchemy.DateTime
    }
    conn = engine.connect()
    if if_exist == False:
        df.to_sql(sql_id + '_met', con=conn, if_exists='replace', # if table does not exist, create table for station
            index=False,dtype=dtype)
        # print(0)
        conn.close()
    else:
        df.to_sql(con=conn, name='sql_temp', if_exists='replace', # create temporary table using df.to_sql
            index=False,dtype=dtype) 
        qry_str = "INSERT INTO public.{sql_id} SELECT * FROM sql_temp EXCEPT SELECT * FROM public.{sql_id};".format(sql_id = sql_id+'_met') 
        conn.execute(text(qry_str))
        conn.commit()
        conn.close()
        # print(1)

def push_sql_func(staname, sta_id, sql_id,df,engine,if_exist):
    if df.empty:
        
        print('No data to upload')
    else:
        df['TIMESTAMP'] = df['TIMESTAMP'].apply(convert_datetime) # apply convert_datetime function to convert 24:00:00 to 00:00:00 the following day
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        df.columns = [x.lower() for x in df.columns]
        df.insert(0,'staname','')
        df.insert(0,'sql_id','')
        df.insert(0,'sta_id','')
        df['sta_id'] = sta_id
        df['staname'] = staname
        df['sql_id'] = sql_id
        df = df.iloc[::-1]

        # print(df)
        dtype = {
            "timestamp": sqlalchemy.DateTime
        }

        to_sql_func(df,engine,if_exist)
        
        print('commited data')

def init_func(staname, sta_id, sql_id, engine):
    conn = engine.connect()
    qry_if_exist = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{sql_id}');".format(sql_id = sql_id+'_met')
    resoverall = conn.execute(text(qry_if_exist))
    conn.close()
    if_exist = resoverall.first()[0]
    end_date = dt.date.today()
    start_date = end_date - timedelta(days=1)
    df = to_df_func(sta_id, sql_id, staname, start_date,end_date)
    push_sql_func(staname, sta_id, sql_id,df,engine,if_exist)



server = Flask(__name__)

app = Dash(name='Bootstrap_docker_app',
                server=server)

app.layout = html.Div(
    html.Div([
        html.H4('Live Mesonet Updates'),
        html.Div(id='live-update-text'),
        dcc.Interval(
            id='interval-component',
#             interval=300*1000, # in milliseconds
            interval=60*1000, # in milliseconds
            n_intervals=0
        )
    ])
)


@callback(Output('live-update-text', 'children'),
              Input('interval-component', 'n_intervals'))
              
def update_metrics(n):
    #%% DEFINE THE DATABASE CREDENTIALS
    # user = 'postgres'
    # password = 'meso_admin'
    # host = 'localhost'
    # port = 5432
    # database = 'meso_test_db'

    user = 'doadmin'
    password = 'AVNS_Hsz-nqzOy7WLE027Jog'
    host = 'hawaii-mesonet-do-user-14120648-0.b.db.ondigitalocean.com'
    port = 25060
    database = 'sta_met_live'

    engine = create_engine(
            url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, database
            )
        )

    conn = engine.connect()
#     sta_qry = "SELECT * FROM station_data WHERE NOT id = 141;"
    sta_qry = "SELECT * FROM station_data LIMIT 3;" # qry station metadata from station_data table and retrun df
    resoverall = conn.execute(text(sta_qry))
    df_sta_info = DataFrame(resoverall.fetchall())
    df_sta_info.columns = resoverall.keys()
    conn.close()

    for ind, elem in enumerate(df_sta_info['staname']):
        staname = df_sta_info['staname'][ind]
        sta_id = df_sta_info['sta_id'][ind]
        sql_id = df_sta_info['sql_id'][ind]
        init_func(staname,sta_id,sql_id,engine)

if __name__ == '__main__':
    app.run_server(debug=True)
