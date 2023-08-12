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
    

def get_data_vol(select,condition,input_values):
    
    select_query = """SELECT """ + select + """, 
                AVG(UNDERLYING_GROWTH_VOL_PCT),
                 AVG(CONSUMER_BEHAVIOUR_VOL_PCT), 
                 AVG(MACRO_ECONOMIC_VARS_VOL_PCT), 
                 AVG(MARKETING_MIX_VOL_PCT), 
                 AVG(WEATHER_VOL_PCT)
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
                and SECTOR_SCENARIO_CD = ? """+ condition
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


def get_data_val(select,condition,input_values):
    
    select_query = """SELECT """ + select + """, 
                AVG(UNDERLYING_GROWTH_VAL_PCT),
                 AVG(CONSUMER_BEHAVIOUR_VAL_PCT), 
                 AVG(MACRO_ECONOMIC_VARS_VAL_PCT), 
                 AVG(MARKETING_MIX_VAL_PCT), 
                 AVG(WEATHER_VAL_PCT)
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
                and SECTOR_SCENARIO_CD = ? """+ condition
    
   
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
    
    
    column_names = ["YEAR","PERIOD_TYPE","Underlying Growth","Consumber Behaviour Changes",
               "Macro Economic Variables","Marketing Mix","Weather"]
   
    metric = metric.lower()
    
    if metric == "volume":
        
        selectQ = "BASE_YEAR, QTR"
        conditionQ ="group by BASE_YEAR, QTR order by BASE_YEAR, QTR;"
        QDF = pd.DataFrame(get_data_vol(selectQ,conditionQ,input_values),columns=column_names)
        #print(QDF)
        
        selectY = "BASE_YEAR"
        conditionY ="group by BASE_YEAR order by BASE_YEAR;"
        Year = pd.DataFrame(get_data_vol(selectY,conditionY,input_values),columns=column_names[:1]+column_names[2:])
        Year["PERIOD_TYPE"] = "YEAR"
        
        selectH1 = "BASE_YEAR, 'H1'"
        conditionH1 ="and QTR in ('Q1','Q2') group by BASE_YEAR order by BASE_YEAR;"
        H1 = pd.DataFrame(get_data_vol(selectH1,conditionH1,input_values),columns=column_names)
        
        selectH2 = "BASE_YEAR, 'H2'"
        conditionH2 ="and QTR in ('Q3','Q4') group by BASE_YEAR order by BASE_YEAR;"
        H2 = pd.DataFrame(get_data_vol(selectH2,conditionH2,input_values),columns=column_names)
        
        masterDF = pd.concat([QDF,H1,H2,Year], ignore_index=True,sort=False)
        
    if metric =="value":    
        
        selectQ = "BASE_YEAR, QTR"
        conditionQ ="group by BASE_YEAR, QTR order by BASE_YEAR, QTR;"
        QDF = pd.DataFrame(get_data_val(selectQ,conditionQ,input_values),columns=column_names)
        #print(QDF)
        
        selectY = "BASE_YEAR"
        conditionY ="group by BASE_YEAR order by BASE_YEAR;"
        Year = pd.DataFrame(get_data_val(selectY,conditionY,input_values),columns=column_names[:1]+column_names[2:])
        Year["PERIOD_TYPE"] = "YEAR"
        
        selectH1 = "BASE_YEAR, 'H1'"
        conditionH1 ="and QTR in ('Q1','Q2') group by BASE_YEAR order by BASE_YEAR;"
        H1 = pd.DataFrame(get_data_val(selectH1,conditionH1,input_values),columns=column_names)
        
        selectH2 = "BASE_YEAR, 'H2'"
        conditionH2 ="and QTR in ('Q3','Q4') group by BASE_YEAR order by BASE_YEAR;"
        H2 = pd.DataFrame(get_data_val(selectH2,conditionH2,input_values),columns=column_names)
        
        masterDF = pd.concat([QDF,H1,H2,Year], ignore_index=True,sort=False)
        
        
    
    
    for i in range(2, len(column_names)):
        masterDF[column_names[i]] = masterDF[column_names[i]].astype(float).round(2)

    #masterDF.drop(['Overall Volume Growth'],axis=1,inplace=True)
    #masterDF.insert(2,'Overall Volume Growth', round(masterDF[column_names[3:]].sum(axis=1),2))   
    
    if output == "graph":
         masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['Q1','Q2','Q3','Q4'])]
        
    responseDict = masterDF.to_dict(orient='records')
    responseData ={"request_header":request_header,
               "sales_volume": responseDict}
    
    json_object = json.dumps(responseData)
    return(json_object)






