# -*- coding: utf-8 -*-
"""
Created on Sat Aug 14 08:42:44 2021

@author: Rameshkumar
"""


import pandas as pd
# from hdbcli import dbapi
# import numpy as np
import json
import sys
# from flask import jsonify

from sap_hana_credentials import connection
#hana_db ='ULGROWTH20'

try:
    db_config_file = open("UL_DB_CONFIG.json")
    db_config_json = json.load(db_config_file)
    hana_db = db_config_json[0]['DB_NAME']
    db_config_file.close()
except:
    print("Error reading config file")
    sys.exit()
    

def get_data_vol(input_values):
    
    
    select_query = """SELECT BASE_YEAR,PERIOD,OVERALL_VOLUME_GROWTH_VOL_PCT, 
                     UNDERLYING_GROWTH_VOL_PCT,
                     CONSUMER_BEHAVIOUR_VOL_PCT, 
                     MACRO_ECONOMIC_VARS_VOL_PCT, 
                     MARKETING_MIX_VOL_PCT, 
                     WEATHER_VOL_PCT
                from """+hana_db+""".res_qtr_growth_pct 
                where UL_GEO_ID in
                	(SELECT B.UL_RGN_GEO_ID 
                	   FROM """+hana_db+""".ul_geo_hier B 
                		JOIN """+hana_db+""".geo_hier C 
                		ON B.CTRY_GEO_ID = C.GEO_ID 
                		AND C.REGION_NAME = 'DUMMY' 
                		AND C.GEO_ID = %s)
                and CATG_CD = %s
                and SUBCAT_CD = %s
                and SECTOR_SCENARIO_CD = %s """
    
    print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


def get_data_val(input_values):
    
    select_query = """SELECT BASE_YEAR,PERIOD,OVERALL_VOLUME_GROWTH_VAL_PCT, 
                       UNDERLYING_GROWTH_VAL_PCT,
                       CONSUMER_BEHAVIOUR_VAL_PCT, 
                       MACRO_ECONOMIC_VARS_VAL_PCT, 
                       MARKETING_MIX_VAL_PCT, 
                       WEATHER_VAL_PCT
                from """+hana_db+""".RES_QTR_GROWTH_PCT
                where UL_GEO_ID in
                	(SELECT B.UL_RGN_GEO_ID 
                	   FROM """+hana_db+""".UL_GEO_HIER B 
                		JOIN """+hana_db+""".GEO_HIER C 
                		ON B.CTRY_GEO_ID = C.GEO_ID 
                		AND C.REGION_NAME = 'DUMMY' 
                		AND C.GEO_ID = ?)
                and CATG_CD = ?
                and SUBCAT_CD = ?
                and SECTOR_SCENARIO_CD = ? """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows




def ul_db_input_contribution(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,output):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario, "METRIC":metric}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    
    column_names = ["YEAR","PERIOD_TYPE","Overall Volume Growth","Underlying Growth","Consumber Behaviour Changes",
               "Macro Economic Variables","Marketing Mix","Weather"]
    
   
    metric = metric.lower()    
    
    if metric == "volume":
        #return "hi"
        masterDF = pd.DataFrame(get_data_vol(input_values),columns=column_names)
        #return "hi1"
    
    if metric =="value":    
        masterDF = pd.DataFrame(get_data_val(input_values),columns=column_names)
        
    
    for i in range(2, len(column_names)):
        masterDF[column_names[i]] = masterDF[column_names[i]].astype(float).round(2)

    #masterDF.drop(['Overall Volume Growth'],axis=1,inplace=True)
    #masterDF.insert(2,'Overall Volume Growth', round(masterDF[column_names[3:]].sum(axis=1),2))   
    masterDF.replace('FY','YEAR',inplace=True)
    masterDF.drop(masterDF[(masterDF['YEAR']==2023) & (masterDF['PERIOD_TYPE']=='YEAR')].index,inplace = True)

    
    if output == "graph":
         masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['Q1','Q2','Q3','Q4'])]
         masterDF.drop(columns=["Overall Volume Growth"],inplace = True)
        
    responseDict = masterDF.to_dict(orient='records')
    responseData ={"request_header":request_header,
               "sales_volume": responseDict}
    
    json_object = json.dumps(responseData)
    return(json_object)


#output = ul_db_input_contribution('142','1','1','9','1','volume','graph')
#print(output)

        
