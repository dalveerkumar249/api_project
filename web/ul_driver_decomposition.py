# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 10:40:25 2021

@author: Rameshkumar

#08-Apr-2022 

active API calls in the front end
- ul_db_driver_by_quarter
- ul_db_driver_by_quarter_pct_growth



"""


import pandas as pd
# from hdbcli import dbapi
# import numpy as np
import json
from pandas.io.json import json_normalize
import sys
# from flask import jsonify
#hana_db ='ULGROWTH20'


from sap_hana_credentials import connection

try:
    db_config_file = open("UL_DB_CONFIG.json")
    db_config_json = json.load(db_config_file)
    hana_db = db_config_json[0]['DB_NAME']
    db_config_file.close()
except:
    print("Error reading config file")
    sys.exit()


def get_data_vol(input_values):
    
    select_query = """SELECT a.YEAR_FREQ, cast(JSON_VALUE(b.CONTENTS_JSON_VOLUME, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                    a.UNDERLYING_GROWTH_VOL,a.CONSUMER_BEHAVIOUR_VOL,a.MACRO_ECONOMIC_VARS_VOL,a.MARKETING_MIX_VOL,a.WEATHER_VOL
                    FROM """+hana_db+""".res_qtr_decompose a
                    JOIN """+hana_db+""".res_drv_qtr_decompose b on a. UL_GEO_ID = b. UL_GEO_ID and a.CATG_CD = b.CATG_CD 
                    and a.CHNL_CD = b. CHNL_CD and a.FMT_CD = b.FMT_CD and a.SUBCAT_CD = b. SUBCAT_CD and a.SECTOR_SCENARIO_CD = b.SECTOR_SCENARIO_CD
                    and a.YEAR_FREQ = b. YEAR_FREQ
                    where a.UL_GEO_ID in
                    	(SELECT C.UL_RGN_GEO_ID 
                    	   FROM """+hana_db+""".ul_geo_hier C 
                    		JOIN """+hana_db+""".geo_hier D 
                    		ON C.CTRY_GEO_ID = D.GEO_ID 
                    		AND D.REGION_NAME = 'DUMMY' 
                    		AND D.GEO_ID = %s)
                    and a.CATG_CD = %s
                    and a.SUBCAT_CD = %s
                    and a.SECTOR_SCENARIO_CD = %s """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows



def get_data_vol_pct_change(input_values):
    
    # select_query = """SELECT a.BASE_YEAR,a.COMP_YEAR,a.PERIOD, cast(JSON_VALUE(b.CONTENTS_JSON_VOLUME, '$.SALES_VOLUME') as decimal) as SALES_VOLUME,
    #                 a.UNDERLYING_GROWTH_VOL_PCT,a.CONSUMER_BEHAVIOUR_VOL_PCT,a.MACRO_ECONOMIC_VARS_VOL_PCT,a.MARKETING_MIX_VOL_PCT,a.WEATHER_VOL_PCT
    #                 FROM """+hana_db+""".RES_QTR_GROWTH_PCT a
    #                 JOIN """+hana_db+""".RES_DRV_QTR_GROWTH_PCT b on a. UL_GEO_ID = b. UL_GEO_ID and a.CATG_CD = b.CATG_CD 
    #                 and a.CHNL_CD = b. CHNL_CD and a.FMT_CD = b.FMT_CD and a.SUBCAT_CD = b. SUBCAT_CD and a.SECTOR_SCENARIO_CD = b.SECTOR_SCENARIO_CD
    #                 and a.BASE_YEAR = b. BASE_YEAR and a.COMP_YEAR = b.COMP_YEAR and a.PERIOD = b. PERIOD
    #                 where a.UL_GEO_ID in
    #                 	(SELECT C.UL_RGN_GEO_ID 
    #                 	   FROM """+hana_db+""".UL_GEO_HIER C 
    #                 		JOIN """+hana_db+""".GEO_HIER D 
    #                 		ON C.CTRY_GEO_ID = D.GEO_ID 
    #                 		AND D.REGION_NAME = 'DUMMY' 
    #                 		AND D.GEO_ID = ?)
    #                 and a.CATG_CD = ?
    #                 and a.SUBCAT_CD = ?
    #                 and a.SECTOR_SCENARIO_CD = ?
    #                 order by a.BASE_YEAR,a.COMP_YEAR """


    select_query = """SELECT a.BASE_YEAR,a.COMP_YEAR,a.PERIOD, cast(JSON_VALUE(b.CONTENTS_JSON_VOLUME, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                    a.UNDERLYING_GROWTH_VOL_PCT,a.CONSUMER_BEHAVIOUR_VOL_PCT,a.MACRO_ECONOMIC_VARS_VOL_PCT,a.MARKETING_MIX_VOL_PCT,a.WEATHER_VOL_PCT
                    FROM """+hana_db+""".res_qtr_growth_pct a
                    JOIN """+hana_db+""".res_drv_qtr_growth_pct b on a. UL_GEO_ID = b. UL_GEO_ID and a.CATG_CD = b.CATG_CD
                    and a.CHNL_CD = b. CHNL_CD and a.FMT_CD = b.FMT_CD and a.SUBCAT_CD = b. SUBCAT_CD and a.SECTOR_SCENARIO_CD = b.SECTOR_SCENARIO_CD
                    and a.BASE_YEAR = b. BASE_YEAR and a.COMP_YEAR = b.COMP_YEAR and a.PERIOD = b. PERIOD
                    where a.UL_GEO_ID in
                        (SELECT C.UL_RGN_GEO_ID
                           FROM """+hana_db+""".ul_geo_hier C
                            JOIN """+hana_db+""".geo_hier D
                            ON C.CTRY_GEO_ID = D.GEO_ID
                            AND D.REGION_NAME = 'DUMMY'
                            AND D.GEO_ID = %s)
                    and a.CATG_CD = %s
                    and a.SUBCAT_CD = %s
                    and a.SECTOR_SCENARIO_CD = %s
                    order by a.BASE_YEAR,a.COMP_YEAR """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

def get_driver_data(input_values):
    
    # select_query = """select YEAR_FREQ,CONTENTS_JSON_VOLUME from
    #                     """+hana_db+""".RES_DRV_QTR_DECOMPOSE a
    #                     where a.UL_GEO_ID in
    #                     	(SELECT C.UL_RGN_GEO_ID 
    #                     	   FROM """+hana_db+""".UL_GEO_HIER C 
    #                     		JOIN """+hana_db+""".GEO_HIER D 
    #                     		ON C.CTRY_GEO_ID = D.GEO_ID 
    #                     		AND D.REGION_NAME = 'DUMMY' 
    #                     		AND D.GEO_ID = ?)
    #                     and a.CATG_CD = ?
    #                     and a.SUBCAT_CD = ?
    #                     and a.SECTOR_SCENARIO_CD = ? """

    select_query = """select YEAR_FREQ,CONTENTS_JSON_VOLUME from
                        """+hana_db+""".res_drv_qtr_decompose a
                        where a.UL_GEO_ID in
                            (SELECT C.UL_RGN_GEO_ID
                               FROM """+hana_db+""".ul_geo_hier C
                                JOIN """+hana_db+""".geo_hier D
                                ON C.CTRY_GEO_ID = D.GEO_ID
                                AND D.REGION_NAME = 'DUMMY'
                                AND D.GEO_ID = %s)
                        and a.CATG_CD = %s
                        and a.SUBCAT_CD = %s
                        and a.SECTOR_SCENARIO_CD = %s """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    print(str(select_query))
    return rows

def get_driver_data_pct_growth(input_values):
    
    select_query = """select BASE_YEAR,COMP_YEAR,PERIOD,CONTENTS_JSON_VOLUME from
                        """+hana_db+""".res_drv_qtr_growth_pct a
                        where a.UL_GEO_ID in
                        	(SELECT C.UL_RGN_GEO_ID 
                        	   FROM """+hana_db+""".ul_geo_hier C 
                        		JOIN """+hana_db+""".geo_hier D 
                        		ON C.CTRY_GEO_ID = D.GEO_ID 
                        		AND D.REGION_NAME = 'DUMMY' 
                        		AND D.GEO_ID = %s)
                        and a.CATG_CD = %s
                        and a.SUBCAT_CD = %s
                        and a.SECTOR_SCENARIO_CD = %s """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

def get_driver_data_driver_analysis(driver_name,sub_driver,input_values):
    
    select_query = """SELECT a.BASE_YEAR,a.COMP_YEAR,a.PERIOD,
                    cast(JSON_VALUE(b.CONTENTS_JSON_VOLUME, '$."""+sub_driver+"""') as decimal) as """+sub_driver+""",
                    a."""+driver_name+"""
                    FROM """+hana_db+""".RES_QTR_GROWTH_PCT a
                    JOIN """+hana_db+""".RES_DRV_QTR_GROWTH_PCT b on a. UL_GEO_ID = b. UL_GEO_ID and a.CATG_CD = b.CATG_CD 
                    and a.CHNL_CD = b. CHNL_CD and a.FMT_CD = b.FMT_CD and a.SUBCAT_CD = b. SUBCAT_CD and a.SECTOR_SCENARIO_CD = b.SECTOR_SCENARIO_CD
                    and a.BASE_YEAR = b. BASE_YEAR and a.COMP_YEAR = b.COMP_YEAR and a.PERIOD = b. PERIOD
                    where a.UL_GEO_ID in
                    	(SELECT C.UL_RGN_GEO_ID 
                    	   FROM """+hana_db+""".UL_GEO_HIER C 
                    		JOIN """+hana_db+""".GEO_HIER D 
                    		ON C.CTRY_GEO_ID = D.GEO_ID 
                    		AND D.REGION_NAME = 'DUMMY' 
                    		AND D.GEO_ID = ?)
                    and a.CATG_CD = ?
                    and a.SUBCAT_CD = ?
                    and a.SECTOR_SCENARIO_CD = ? """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

def get_res_summary_by_cat(input_values):
    
    # select_query = """select TOP_PILLARS,VOLUME_FORECAST,UNDERLYING_GROWTH,MARKETING_MIX,
    #                     WEATHER,CONSUMER_BEHAVIOUR,MACRO_ECONOMIC from
    #                     """+hana_db+""".RES_EXPLAIN_SUMMARY a
    #                     where a.UL_GEO_ID in
    #                     	(SELECT C.UL_RGN_GEO_ID 
    #                     	   FROM """+hana_db+""".UL_GEO_HIER C 
    #                     		JOIN """+hana_db+""".GEO_HIER D 
    #                     		ON C.CTRY_GEO_ID = D.GEO_ID 
    #                     		AND D.REGION_NAME = 'DUMMY' 
    #                     		AND D.GEO_ID = ?)
    #                     and a.CATG_CD = ?
    #                     and a.SUBCAT_CD = ?
    #                     and a.SECTOR_SCENARIO_CD = ? """


    select_query = """select TOP_PILLARS,VOLUME_FORECAST,UNDERLYING_GROWTH,MARKETING_MIX,
                        WEATHER,CONSUMER_BEHAVIOUR,MACRO_ECONOMIC from
                        """+hana_db+""".res_explain_summary a
                        where a.UL_GEO_ID in
                            (SELECT C.UL_RGN_GEO_ID
                               FROM """+hana_db+""".ul_geo_hier C
                                JOIN """+hana_db+""".geo_hier D
                                ON C.CTRY_GEO_ID = D.GEO_ID
                                AND D.REGION_NAME = 'DUMMY'
                                AND D.GEO_ID = %s)
                        and a.CATG_CD = %s
                        and a.SUBCAT_CD = %s
                        and a.SECTOR_SCENARIO_CD = %s """
    
   
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

def get_res_qtr_summary_by_cat(input_values):
    
    # select_query = """select YEAR_VS_YEAR,QUARTER,VOLUME,UNDERLYING_GROWTH,MARKETING_MIX,
    #                     WEATHER,CONSUMER_BEHAVIOUR,MACRO_ECONOMIC,TOP_DRIVERS,IMPACTS from
    #                     """+hana_db+""".RES_EXPLAIN_QTR a
    #                     where a.UL_GEO_ID in
    #                     	(SELECT C.UL_RGN_GEO_ID 
    #                     	   FROM """+hana_db+""".UL_GEO_HIER C 
    #                     		JOIN """+hana_db+""".GEO_HIER D 
    #                     		ON C.CTRY_GEO_ID = D.GEO_ID 
    #                     		AND D.REGION_NAME = 'DUMMY' 
    #                     		AND D.GEO_ID = ?)
    #                     and a.CATG_CD = ?
    #                     and a.SUBCAT_CD = ?
    #                     and a.SECTOR_SCENARIO_CD = ? """


    select_query = """select YEAR_VS_YEAR,QUARTER,VOLUME,UNDERLYING_GROWTH,MARKETING_MIX,
                        WEATHER,CONSUMER_BEHAVIOUR,MACRO_ECONOMIC,TOP_DRIVERS,IMPACTS from
                        """+hana_db+""".res_explain_qtr a
                        where a.UL_GEO_ID in
                            (SELECT C.UL_RGN_GEO_ID
                               FROM """+hana_db+""".ul_geo_hier C
                                JOIN """+hana_db+""".geo_hier D
                                ON C.CTRY_GEO_ID = D.GEO_ID
                                AND D.REGION_NAME = 'DUMMY'
                                AND D.GEO_ID = %s)
                        and a.CATG_CD = %s
                        and a.SUBCAT_CD = %s
                        and a.SECTOR_SCENARIO_CD = %s """
    
   
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

def ul_db_volume_decomposition(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names = ["YEAR_FREQ","SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
               "MACRO_ECONOMIC","MARKETING_MIX","WEATHER"]
    
    
    masterDF = pd.DataFrame(get_data_vol(input_values),columns=column_names)
    
    
    if masterDF.shape[0] <1:
        responseData ={"request_header":request_header,
              "VOL_DECOMP": ""}
        json_object = json.dumps(responseData)
        return(json_object)
    
    else:
        masterDF[["YEAR","PERIOD"]] = masterDF["YEAR_FREQ"].str.split("-",expand=True)
        masterDF["YEAR"] = masterDF["YEAR"].astype(int)
        
        masterDF.replace('FY','YEAR',inplace=True)
        
        column_names.remove("YEAR_FREQ")
        masterDF.rename(columns={"PERIOD":"QUARTER"}, inplace=True)
        column_names = ["YEAR","QUARTER"] + column_names
    
        masterDF['PRI'] = 0
        pri_dict = {'Q1':1,'Q2':2,'Q3':3,'Q4':4,'H1':5,'H2':6,'YEAR':7}
        for key in pri_dict:
            masterDF.loc[masterDF["QUARTER"]==key, "PRI"] = pri_dict[key]
    
        
        masterDF = masterDF.sort_values(['YEAR','PRI'])
        masterDF = masterDF[column_names]
        
        for i in range(2, len(column_names)):
                masterDF[column_names[i]] = masterDF[column_names[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]
        masterDF["SALES_VOLUME"] = masterDF["SALES_VOLUME"].astype(float).round(0)
        
        responseDict = masterDF.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "VOL_DECOMP": responseDict}
        
        json_object = json.dumps(responseData)
        return(json_object)


    
def ul_db_volume_decomposition_qtr_change(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names = ["YEAR_FREQ","SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
               "MACRO_ECONOMIC","MARKETING_MIX","WEATHER"]
    
    
    masterDF = pd.DataFrame(get_data_vol(input_values),columns=column_names)
    
    
    if masterDF.shape[0] <1:
        responseData ={"request_header":request_header,
              "VOL_DECOMP_QTR_CHG": ""}
        json_object = json.dumps(responseData)
        return(json_object)
    
    else:
        masterDF[["YEAR","PERIOD"]] = masterDF["YEAR_FREQ"].str.split("-",expand=True)
        masterDF["YEAR"] = masterDF["YEAR"].astype(int)
        
        masterDF.replace('FY','YEAR',inplace=True)
        
        column_names.remove("YEAR_FREQ")
        masterDF.rename(columns={"PERIOD":"QUARTER"}, inplace=True)
        column_names = ["YEAR","QUARTER"] + column_names
    
        masterDF['PRI'] = 0
        pri_dict = {'Q1':1,'Q2':2,'Q3':3,'Q4':4,'H1':5,'H2':6,'YEAR':7}
        for key in pri_dict:
            masterDF.loc[masterDF["QUARTER"]==key, "PRI"] = pri_dict[key]
    
        
        masterDF = masterDF.sort_values(['YEAR','PRI'])
        masterDF = masterDF[column_names]
        key_columns =["SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
               "MACRO_ECONOMIC","MARKETING_MIX","WEATHER"]

        qtr_list = ['Q1','Q2','Q3','Q4','H1','H2','YEAR']
        masterDF.reset_index(drop=True,inplace=True)
        #tempDF.to_csv("driver_decompose.csv")
        masterDF2 = pd.DataFrame()
        starting_year = min(masterDF['YEAR'])+1
        year_list = list(masterDF['YEAR'].unique())
                    
        for year in year_list:
            if year >= starting_year:
                yearDF = masterDF[masterDF["YEAR"]== year]
                compyearDF = masterDF[masterDF["YEAR"]== year-1]
        
                for qtr in qtr_list:
                    yearQ1 = yearDF.loc[yearDF['QUARTER']==qtr]
                    compyearQ1 = compyearDF.loc[compyearDF['QUARTER']==qtr]
                    resultDF = yearQ1.set_index('QUARTER').subtract(compyearQ1.set_index('QUARTER'),axis='columns')
                    resultDF.insert(0,'BASE_YEAR',year)     
                    resultDF.insert(1,'COMP_YEAR',year-1)
                    #resultDF['PERIOD'] = qtr
                    resultDF.reset_index(inplace=True)
                    masterDF2 = masterDF2.append(resultDF)
        
        masterDF2 = masterDF2.dropna(subset=key_columns)
        
        masterDF2.insert(0,"YEAR_COMPARISON",masterDF2['BASE_YEAR'].astype(str) +"_vs_"+ masterDF2['COMP_YEAR'].astype(str))
        masterDF2.drop(columns=["YEAR","BASE_YEAR","COMP_YEAR"],inplace = True)

        
        for i in range(0, len(key_columns)):
                masterDF2[key_columns[i]] = masterDF2[key_columns[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]
        
        masterDF2["SALES_VOLUME"] = masterDF2["SALES_VOLUME"].astype(float).round(0)
        
        responseDict = masterDF2.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "VOL_DECOMP_QTR_CHG": responseDict}
        
        json_object = json.dumps(responseData)
        return(json_object)


def ul_db_volume_decomposition_qtr_pct_change(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,output):
    
#    content = {"COUNTRY_NAME":"142","DIVISION":"1","CATAGORY_NAME":"10","SUB_CATEGORY_NAME":"13",
#                               "FORECAST_SCENARIO": "1","OUTPUT":"Table"}
#    
#    geo_id = content['COUNTRY_NAME']
#    division = content['DIVISION']
#    catg_cd = content['CATAGORY_NAME']
#    sub_catg_cd = content['SUB_CATEGORY_NAME']
#    forecast_secenario = content['FORECAST_SCENARIO']
#    output = content['OUTPUT']
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario,"OUTPUT":output}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    column_names = ["YEAR_VS_YEAR","QUARTER","VOLUME","UNDERLYING_GROWTH","MARKETING_MIX","WEATHER","CONSUMER_BEHAVIOUR","MACRO_ECONOMIC","TOP_DRIVERS","IMPACTS"]

    masterDF = pd.DataFrame(get_res_qtr_summary_by_cat(input_values),columns=column_names)
    
    key_columns =["SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
                   "MACRO_ECONOMIC","MARKETING_MIX","WEATHER","TOP_DRIVERS","IMPACTS"]

    key_columns1 =["SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
                   "MACRO_ECONOMIC","MARKETING_MIX","WEATHER"]
        
    if masterDF.shape[0] > 1:
        
        masterDF[['COMP_YEAR', 'BASE_YEAR']] = masterDF['YEAR_VS_YEAR'].str.split('vs ', 1, expand=True)

        #masterDF.insert(0,"YEAR_COMPARISON",masterDF['YEAR_VS_YEAR'])
        #masterDF["YEAR_COMPARISON"] = masterDF["YEAR_COMPARISON"].apply(lambda x: x.replace(" ","_"))
        masterDF["QUARTER"] = masterDF["QUARTER"].replace({1:'Q1',2:'Q2',3:'Q3',4:'Q4',0:'FY'})
        masterDF.drop(['YEAR_VS_YEAR'],axis =1, inplace=True)
        masterDF.rename(columns={"VOLUME":"SALES_VOLUME"}, inplace=True)
        
    
    
    else:
    
        column_names = ["BASE_YEAR","COMP_YEAR","PERIOD","SALES_VOLUME","UNDERLYING_GROWTH","CONSUMER_BEHAVIOUR",
                   "MACRO_ECONOMIC","MARKETING_MIX","WEATHER"]
        
       
       
        masterDF = pd.DataFrame(get_data_vol_pct_change(input_values),columns=column_names)
        
        
        if masterDF.shape[0] <1:
            responseData ={"request_header":request_header,
                  "VOL_DECOMP_QTR_CHG_PER": "",
                  "CAT_SUMMARY":""}
            json_object = json.dumps(responseData)
            return(json_object)
    
                
        masterDF.rename(columns={"PERIOD":"QUARTER"}, inplace=True)
        masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','FY'])]
        masterDF["TOP_DRIVERS"] = ""
        masterDF["IMPACTS"] = ""
        
    
    masterDF['PRI'] = 0
    pri_dict = {'Q1':1,'Q2':2,'Q3':3,'Q4':4,'FY':5}
   
    for key in pri_dict:
        masterDF.loc[masterDF["QUARTER"]==key, "PRI"] = pri_dict[key]
        
    masterDF = masterDF.sort_values(['BASE_YEAR','PRI'])
    #print(masterDF)
    output = output.lower()
  
    if output == 'graph':
       
        masterDF.drop(['TOP_DRIVERS','IMPACTS'],axis =1, inplace=True)
        
        masterDF['BASE_YEAR'] = masterDF['BASE_YEAR'].astype(str)
        masterDF['BASE_YEAR'] = [x.strip()[2:] for x in masterDF['BASE_YEAR']]
        
        masterDF['COMP_YEAR'] = masterDF['COMP_YEAR'].astype(str)
        masterDF['COMP_YEAR'] = [x.strip()[2:] for x in masterDF['COMP_YEAR']]
        
        masterDF.insert(0,"YEAR_COMPARISON",masterDF['BASE_YEAR'].astype(str) +"-vs-"+ masterDF['COMP_YEAR'].astype(str))
    
        column_names = ["YEAR_COMPARISON","QUARTER"] + key_columns1

        masterDF = masterDF[column_names]
            

        for i in range(0, len(key_columns)-2):
                masterDF[key_columns[i]] = masterDF[key_columns[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]

        masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4'])]
        masterDF.drop(columns=["SALES_VOLUME"],inplace = True)
        
        responseDict = masterDF.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "VOL_DECOMP_QTR_CHG_PER": responseDict
                  }
        
        json_object = json.dumps(responseData)
        return(json_object)
    
    
    
    else:
    
        masterDF.insert(0,"YEAR_COMPARISON",masterDF['BASE_YEAR'].astype(str) +"_vs_"+ masterDF['COMP_YEAR'].astype(str))
        column_names = ["YEAR_COMPARISON","QUARTER"] + key_columns

        masterDF = masterDF[column_names]
                
    
        for i in range(0, len(key_columns)-2):
                masterDF[key_columns[i]] = masterDF[key_columns[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]
        
        column_names = ["TOP_PILLARS","VOLUME_FORECAST","UNDERLYING_GROWTH","MARKETING_MIX","WEATHER","CONSUMER_BEHAVIOUR","MACRO_ECONOMIC"]
        
        masterDF2 = pd.DataFrame(get_res_summary_by_cat(input_values),columns=column_names)
        
        if masterDF2.shape[0] > 0:
            top_pillars = masterDF2['TOP_PILLARS'].loc[0]
            vol_forecast = masterDF2['VOLUME_FORECAST'].loc[0]
            
            masterDF2['SALES_VOLUME'] = "TOP_PILLARS :"+ top_pillars +" VOLUME_FORECAST :" + vol_forecast
            masterDF2.drop(['TOP_PILLARS','VOLUME_FORECAST'],axis = 1, inplace=True)
            responseDict1 = masterDF2.to_dict(orient='records')
        else:
            responseDict1 = ''

    
        responseDict = masterDF.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "VOL_DECOMP_QTR_CHG_PER": responseDict,
                  "CAT_SUMMARY":responseDict1}
        
        json_object = json.dumps(responseData)
        return(json_object)
    
    

def ul_db_driver_by_quarter(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names = ["YEAR_FREQ","CONTENTS_JSON_VOLUME"]
    
    
    masterDF = pd.DataFrame(get_driver_data(input_values),columns=column_names)
    
    
    if masterDF.shape[0] <1:
        responseData ={"request_header":request_header,
              "INPUT_DRIVER_ACTUAL_VAL": ""}
        json_object = json.dumps(responseData)
        return(json_object)
    
    else:
        masterDF[["YEAR","QUARTER"]] = masterDF["YEAR_FREQ"].str.split("-",expand=True)
        masterDF["YEAR"] = masterDF["YEAR"].astype(int)
        
        masterDF.replace('FY','YEAR',inplace=True)
        
        masterDF2 = pd.DataFrame()
        for index, rows in masterDF.iterrows():
            data = json.loads(rows['CONTENTS_JSON_VOLUME'])
            #result = pd.json_normalize(data, max_level=0)
            result = json_normalize(data, max_level=0)
            #result.insert(0,"YEAR",rows['YEAR'])
            result.insert(1,"QUARTER",rows['QUARTER'])
            masterDF2 = masterDF2.append(result)
        
        masterDF2['PRI'] = 0
        pri_dict = {'Q1':1,'Q2':2,'Q3':3,'Q4':4,'H1':5,'H2':6,'YEAR':7}
        for key in pri_dict:
            masterDF2.loc[masterDF2["QUARTER"]==key, "PRI"] = pri_dict[key]
        
        masterDF2 = masterDF2.sort_values(['YEAR','PRI'])
        masterDF2.drop(columns=["PRI"],inplace = True)      
        
        key_columns = list(masterDF2.columns)
        
        for i in range(2, len(key_columns)):
                masterDF2[key_columns[i]] = masterDF2[key_columns[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]
        
        responseDict = masterDF2.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "INPUT_DRIVER_ACTUAL_VAL": responseDict}
        
        json_object = json.dumps(responseData)
        return(json_object)


def ul_db_driver_by_quarter_pct_growth(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario}
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names = ["BASE_YEAR","COMP_YEAR","PERIOD","CONTENTS_JSON_VOLUME"]
    
    
    masterDF = pd.DataFrame(get_driver_data_pct_growth(input_values),columns=column_names)
    
    
    if masterDF.shape[0] <1:
        responseData ={"request_header":request_header,
              "INPUT_DRIVER_ACTUAL_VAL_PER_CHG": ""}
        json_object = json.dumps(responseData)
        return(json_object)
    
    else:
        masterDF.rename(columns={"PERIOD":"QUARTER"}, inplace=True)
        masterDF.insert(0,"YEAR_COMPARISON",masterDF['BASE_YEAR'].astype(str) +"_vs_"+ masterDF['COMP_YEAR'].astype(str))

        masterDF2 = pd.DataFrame()
        for index, rows in masterDF.iterrows():
            data = json.loads(rows['CONTENTS_JSON_VOLUME'])
            #result = pd.json_normalize(data, max_level=0)
            result = json_normalize(data, max_level=0)
            
            result.insert(0,"YEAR_COMPARISON",rows['YEAR_COMPARISON'])
            result.insert(1,"QUARTER",rows['QUARTER'])
            masterDF2 = masterDF2.append(result)
        
        
        key_columns = list(masterDF2.columns)
        
        for i in range(2, len(key_columns)):
                masterDF2[key_columns[i]] = masterDF2[key_columns[i]].astype(float).round(2)
        #masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4','YEAR'])]
        
       
        responseDict = masterDF2.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "INPUT_DRIVER_ACTUAL_VAL_PER_CHG": responseDict
                  }
        
        json_object = json.dumps(responseData)
        return(json_object)


def ul_db_driver_analysis(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,driver,sub_driver):
    
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario,"DRIVER":driver,"SUB_DRIVER":sub_driver}
    
    driver = driver.lower()
    #sub_driver = sub_driver.lower()
    driver_dict ={"consumer behaviour":"CONSUMER_BEHAVIOUR_VOL_PCT",
                  "underlying growth":"UNDERLYING_GROWTH_VOL_PCT",
                  "macro":"MACRO_ECONOMIC_VARS_VOL_PCT",
                  "marketing mix":"MARKETING_MIX_VOL_PCT",
                  "weather":"WEATHER_VOL_PCT"
            }
    
    for key in driver_dict:
        if key == driver:
            driver_name = driver_dict[key]
    
    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names = ["BASE_YEAR","COMP_YEAR","PERIOD",sub_driver,driver]
    
    
    masterDF = pd.DataFrame(get_driver_data_driver_analysis(driver_name,sub_driver,input_values),columns=column_names)
    
    
    if masterDF.shape[0] <1:
        responseData ={"request_header":request_header,
              "DRIVER_ANALYSIS": ""}
        json_object = json.dumps(responseData)
        return(json_object)
    
    else:
        masterDF.rename(columns={"PERIOD":"QUARTER"}, inplace=True)
        masterDF.insert(0,"YEAR_COMPARISON",masterDF['BASE_YEAR'].astype(str) +"_vs_"+ masterDF['COMP_YEAR'].astype(str))
        masterDF.drop(columns=["BASE_YEAR","COMP_YEAR"],inplace = True)
               
        key_columns = list(masterDF.columns)
        
        for i in range(2, len(key_columns)):
                masterDF[key_columns[i]] = masterDF[key_columns[i]].astype(float).round(2)
        masterDF = masterDF.loc[masterDF['QUARTER'].isin(['Q1','Q2','Q3','Q4'])]
        
        for i in range(2, len(key_columns)):
                masterDF[key_columns[i]] = masterDF[key_columns[i]].astype(str) + '%'
        
        
        
        responseDict = masterDF.to_dict(orient='records')
        responseData ={"request_header":request_header,
                  "DRIVER_ANALYSIS": responseDict}
        
        json_object = json.dumps(responseData)
        return(json_object)


def ul_db_sub_driver_list(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,driver):
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                           "FORECAST_SCENARIO": forecast_secenario,"DRIVER":driver}
     
    
    df1= pd.read_excel("Subcat_driver_mapping_decomposition_online.xlsx",sheet_name="subcat_driver_mapping")
    #df1['PERIOD_ENDING_DATE'] = df1["PERIOD_ENDING_DATE"].dt.strftime("%m/%d/%Y")
    driver_name = driver.lower()
    driver_list = df1[(df1['GEO_ID']==int(geo_id)) & (df1['SUBCAT_CD']==int(sub_catg_cd))][driver_name].values[0]
    driver_list = driver_list.split(',')
    sales_vol_dict = {"SUB_DRIVER_LIST":driver_list}
    if len(driver_list) <1:
        responseData ={"request_header":request_header,
                  "SUB_DRIVER_LIST": ""}
    else:
        responseData ={"request_header":request_header,
                  "SUB_DRIVER_LIST": sales_vol_dict}
    
    json_object = json.dumps(responseData)
    return(json_object)
    



#content = {"COUNTRY_NAME":"142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME":"10",
#                           "FORECAST_SCENARIO": "1"}
#
#geo_id = content['COUNTRY_NAME']
#division = content['DIVISION']
#catg_cd = content['CATAGORY_NAME']
#sub_catg_cd = content['SUB_CATEGORY_NAME']
#forecast_secenario = content['FORECAST_SCENARIO']
    

#tempDF = ul_db_driver_by_quarter_pct_growth("179","1","8","1","1")
#tempDF = ul_db_volume_decomposition_qtr_pct_change("169","1","8","1","1","TABLE")
#tempDF = ul_db_driver_by_quarter("179","1","8","1","1")




 


        