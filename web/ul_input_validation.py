# -*- coding: utf-8 -*-
"""
Created on Tue Aug 17 12:37:13 2021

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
    

try:
    config_file = open("UL_API_CONFIG.json")
    config_json = json.load(config_file)
    config_file.close()
except:
    print("Error reading config file")

def overlap_region_check(geo_id):
    
    check_query = """ select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map
                    where ul_rgn_geo_id = """ + str(geo_id) + """ and off_rgn_geo_id = 0; """
    
    cursor_data = connection.cursor()
    cursor_data.execute(check_query)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #print(rows2)
    if len(rows) > 0:
        return True
    else:
        return False
    

def get_sales_data_rgn(selectq1,selectq2,selectq3,conditionq,input_values):
    
    select_query ="""select MONTH_YEAR, """ +selectq1+""",""" +selectq2+""",RECORD_TYPE
            FROM
            (select a.PERIOD_ENDING_DATE as MONTH_YEAR, 
            """+selectq3+""",
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name != 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            AND a.SECTOR_SCENARIO_CD = 1
            """+conditionq+"""
            group by a.period_ending_date,JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
            union
            select a.period_ending_date as MONTH_YEAR,
             """+selectq3+""",
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name != 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            AND a.SECTOR_SCENARIO_CD = %s
            """+conditionq+"""
            group by a.period_ending_date,JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
            )
            order by MONTH_YEAR"""
            

    #print(input_values)
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows
                    
    
    
    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


def get_input_variables_data_rgn(selectq,selectq1,condition,input_values):
    
    # update query to get data based on forecast scenario and COVID_FLAG
    select_query = """Select MONTH_YEAR,"""+selectq+"""
            from
            (
            SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR, 
            """+selectq1+"""
            FROM """+hana_db+""".RES_RECORDSET_SAVE a
            JOIN """+hana_db+""".CATEGORY_TYPE c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            AND a.CATG_CD = %s
            AND a.SUBCAT_CD = %s
            AND a.SECTOR_SCENARIO_CD = 1 
            and a.CHNL_CD = 0
            and a.FMT_CD = 0
            """+condition+"""
            group by a.PERIOD_ENDING_DATE
            union
            SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR, 
            """+selectq1+"""
            FROM """+hana_db+""".RES_RECORDSET_SAVE a
            JOIN """+hana_db+""".CATEGORY_TYPE c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            AND a.CATG_CD = %s
            AND a.SUBCAT_CD = %s
            AND a.SECTOR_SCENARIO_CD = %s 
            and a.CHNL_CD = 0
            and a.FMT_CD = 0
            """+condition+"""
            group by a.PERIOD_ENDING_DATE
            )
            ORDER BY MONTH_YEAR;"""
                
        
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


def get_sales_data_chn_fmt(selectq1,selectq2,selectq3,input_values):
     
    select_query ="""select MONTH_YEAR, """ +selectq1+""",""" +selectq2+""",RECORD_TYPE
            FROM
            (select a.PERIOD_ENDING_DATE as MONTH_YEAR, 
            """+selectq3+""",
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            AND a.SECTOR_SCENARIO_CD = 1
            and a.chnl_cd = %s
            and a.fmt_cd = %s
            
            union
            select a.period_ending_date as MONTH_YEAR,
             """+selectq3+""",
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            AND a.SECTOR_SCENARIO_CD = %s
            and a.chnl_cd = %s
            and a.fmt_cd = %s
            ) sub
            order by MONTH_YEAR"""
            

    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows





def get_input_variables_data_chn_fmt(selectq,selectq1,input_values):
    
    
    
    # update query to get data based on forecast scenario and COVID_FLAG
    select_query = """Select MONTH_YEAR,"""+selectq+"""
                        from
                        (
                        SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR, 
                        """+selectq1+"""
                        FROM """+hana_db+""".res_recordset_save a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = 1 
                        and a.CHNL_CD = %s
                        and a.FMT_CD = %s
                        union
                        SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR, 
                        """+selectq1+"""
                        FROM """+hana_db+""".res_recordset_save a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.CHNL_CD = %s
                        and a.FMT_CD = %s
                        ) sub
                        ORDER BY MONTH_YEAR;"""
    
   
    
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows



# sample input {"COUNTRY_NAME" : "1","DIVISION":"1","CATEGORY_NAME":"1","SUB_CATEGORY":"9","METRIC":"VOLUME","CHANNEL":"1","FORECAST_SCENARIO" : "1"}
def ul_db_input_validation(parameters):

    """
    it will get the input values from DB which are used in the modeling
    
    Augs:
        geo_id(int):country id
        division: dvision code 
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_secenario : forecast scenario code 
        chennel : chennel code
        region : region code
        format :format code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online
        
    """
    
    #These are values from frontend dropdowns and switches
    if type(parameters) == list:
        # param_len = len(parameters)
        geo_id = parameters[0]['COUNTRY_NAME']
        division = parameters[0]['DIVISION']
        catg_cd = parameters[0]['CATAGORY_NAME']
        sub_catg_cd = parameters[0]['SUB_CATEGORY_NAME']
        forecast_scenario = parameters[0]['FORECAST_SCENARIO']
        #forecast_type = parameters[0]['FORECAST_TYPE']
        #channel_cd = 1
        channel_cd = parameters[0]['CHANNEL']
        region_cd = parameters[0]['REGION']
        format_cd = parameters[0]['FORMAT']
        #region_cd = 0
        #format_cd = 0
        metric = parameters[0]['METRIC']
    
    else:
        geo_id = parameters['COUNTRY_NAME']
        division = parameters['DIVISION']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_scenario = parameters['FORECAST_SCENARIO']
        #forecast_type = parameters['FORECAST_TYPE']
        #channel_cd = 1
        channel_cd = parameters['CHANNEL']
        region_cd = parameters['REGION']
        format_cd = parameters['FORMAT']
        #region_cd = 0
        #format_cd = 0
        metric = parameters['METRIC']
     
   
    
    
    for i in range(len(config_json)):
        # print(config_json[i]['geo_id'])
        if str(config_json[i]['geo_id']) == str(geo_id):            
            input_var = config_json[i]["Input_validation"]["Variables"]
            column_names2 = config_json[i]["Input_validation"]["Display_name"]
            calc_metric = config_json[i]["Input_validation"]["metric"]
    
    input_selectQ1 = ""
    input_selectQ2 = ""
    input_selectQ = ""
    input_var_len = len(input_var)
    
         
    for j in range(0, input_var_len):
        if j == input_var_len -1:
            input_selectQ = input_selectQ + input_var[j]
            input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var[j]+"') as decimal),0) as "+input_var[j]
            input_selectQ2 = input_selectQ2 + calc_metric[j]+"(IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var[j]+"') as decimal),0)) as "+input_var[j]
        else:
            input_selectQ = input_selectQ + input_var[j]+","
            input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var[j]+"') as decimal),0) as "+input_var[j]+","
            input_selectQ2 = input_selectQ2 + calc_metric[j]+"(IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var[j]+"') as decimal),0)) as "+input_var[j]+","
    
    
    column_names1 = ["PERIOD_ENDING_DATE","SALES_VOLUME" ,"PREDICTED_VOLUME","RECORD_TYPE"] 

    #return "bobo"
    
    # COVID_FLAG will be included only data available in the DB
#    column_names2 = ["PERIOD_ENDING_DATE","SEASONALITY_NEW" ,"HOLIDAY_CD","PRICE_PER_VOL",
#                    "TDP","AVG_TEMP_CELSIUS","RETAIL_AND_RECREATION_PCT_CHANGE","RESIDENTIAL_PCT_CHANGE",
#                    "PERSONAL_DISPOSABLE_INCOME_REAL_LCU","GDP_NOMINAL_LCU","UNEMP_RATE","PREF_VALUE","COVID_FLAG"] 

    
    column_names2 = ["PERIOD_ENDING_DATE"] + input_var
   
    metric = metric.lower()
    
    if int(channel_cd) > 0 or int(format_cd) > 0:
        #print("inside channel")
        # forecast scenario needs to be added once data is available in the backend 
        
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,channel_cd,format_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,channel_cd,format_cd)
        
        if metric == "value":
            
            selectq1 ="SALES_VALUE"
            selectq2 = "PREDICTED_VALUE"
            selectq3 = """(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""
        
            resultDF1 = pd.DataFrame(get_sales_data_chn_fmt(selectq1,selectq2,selectq3,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_chn_fmt(input_selectQ,input_selectQ1,input_values1),columns=column_names2)
        
        if  metric == "volume":
            
            selectq1 ="SALES_VOLUME"
            selectq2 = "PREDICTED_VOLUME"
            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal) as SALES_VOLUME,
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME"""
        
            
            resultDF1 = pd.DataFrame(get_sales_data_chn_fmt(selectq1,selectq2,selectq3,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_chn_fmt(input_selectQ,input_selectQ1,input_values1),columns=column_names2)

   
    elif int(region_cd) > 0:
        
        #print("inside region")
        #input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,region_cd)
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,region_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,region_cd)

        if overlap_region_check(region_cd):
            
            conditionQ = """and a.ul_geo_id in (select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map
                        where ul_rgn_geo_id = %s and off_rgn_geo_id = 0)"""
                        
        else:
            conditionQ = """and a.ul_geo_id in (%s)"""
        
        if metric == "value":
            selectq1 ="SALES_VALUE"
            selectq2 = "PREDICTED_VALUE"
            selectq3 = """sum((cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal))) as SALES_VALUE,
                        sum((cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal))) as PREDICTED_VALUE"""
        
        
            resultDF1 = pd.DataFrame(get_sales_data_rgn(selectq1,selectq2,selectq3,conditionQ,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_rgn(input_selectQ,input_selectQ2,conditionQ,input_values1),columns=column_names2)
            
        if  metric == "volume":
            selectq1 ="SALES_VOLUME"
            selectq2 = "PREDICTED_VOLUME"
            selectq3 = """sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME,
                        sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as PREDICTED_VOLUME"""
             
            resultDF1 = pd.DataFrame(get_sales_data_rgn(selectq1,selectq2,selectq3,conditionQ,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_rgn(input_selectQ,input_selectQ2,conditionQ,input_values1),columns=column_names2)
    
    
    elif int(channel_cd) == 0 and int(format_cd) == 0 and int(region_cd) ==0:
        #print("inside channel")
        # forecast scenario needs to be added once data is available in the backend 
        
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,channel_cd,format_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,channel_cd,format_cd)
        
        if metric == "value":
            
            selectq1 ="SALES_VALUE"
            selectq2 = "PREDICTED_VALUE"
            selectq3 = """(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""
        
            resultDF1 = pd.DataFrame(get_sales_data_chn_fmt(selectq1,selectq2,selectq3,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_chn_fmt(input_selectQ,input_selectQ1,input_values1),columns=column_names2)
        
        if  metric == "volume":
            
            selectq1 ="SALES_VOLUME"
            selectq2 = "PREDICTED_VOLUME"
            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME"""
        
            
            resultDF1 = pd.DataFrame(get_sales_data_chn_fmt(selectq1,selectq2,selectq3,input_values1),columns=column_names1)
            resultDF2 = pd.DataFrame(get_input_variables_data_chn_fmt(input_selectQ,input_selectQ1,input_values1),columns=column_names2)

    
    
    
    else:
        responseData ={"request_header":parameters,
                   "SALES_VOLUME": [],
                   "INPUT_VALUES":[]}
    
        json_object = json.dumps(responseData)
        return(json_object)
    
        

    # Sales value/volume data processing
    #resultDF1.loc[resultDF1["RECORD_TYPE"]=="ACTUAL", "PREDICTED_VOLUME"] = 0
    #resultDF1.loc[resultDF1["RECORD_TYPE"]=="FORECAST", "SALES_VOLUME"] = 0
    resultDF1["PERIOD_ENDING_DATE"] = resultDF1["PERIOD_ENDING_DATE"].apply(lambda x: x.strftime('%m/%d/%Y'))
    resultDF1["SALES_VOLUME"] = resultDF1["SALES_VOLUME"].astype(float).round(2)
    resultDF1["PREDICTED_VOLUME"] = resultDF1["PREDICTED_VOLUME"].astype(float).round(2)    
    #resultDF1["RECORD_TYPE"] = resultDF1["RECORD_TYPE"].str.replace('PREDICTED','FORECAST')
    #input variable data processing
    #print(resultDF1.head())
    #print(resultDF2.head())
    resultDF2["PERIOD_ENDING_DATE"] = resultDF2["PERIOD_ENDING_DATE"].apply(lambda x: x.strftime('%m/%d/%Y'))
    for i in range(1, len(column_names2)):
        resultDF2[column_names2[i]] = resultDF2[column_names2[i]].astype(float).round(2)
    
    
    
    responseDict1 = resultDF1.to_dict(orient='records')
    responseDict2 = resultDF2.to_dict(orient='records')
    
    
    responseData ={"request_header":parameters,
                   "SALES_VOLUME": responseDict1,
                   "INPUT_VALUES":responseDict2}
    
    json_object = json.dumps(responseData)
    return json_object




def ul_db_input_validation_dropdown(geo_id,division,catg_cd,sub_catg_cd,metric):
    
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}

    # to get available channel list 
    select_query = """select distinct c.chnl_cd,c.chnl_desc
                        from """+hana_db+""".channel_type c 
                        join """+hana_db+""".res_recordset_save a on a.chnl_cd = c.chnl_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        and c.geo_id =%s """
    
    
    input_values = (geo_id,division,geo_id,catg_cd,sub_catg_cd,geo_id)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    column_names = ["CHANNEL_CD","CHANNEL_NAME"]
    chennalDF = pd.DataFrame(rows,columns=column_names)
    #print(chennalDF)
    
    # to get available regions  list 
    select_query = """select distinct z.ul_rgn_geo_id,z.ul_region_name 
                    from """+hana_db+""".ul_geo_hier z 
                    join """+hana_db+""".res_recordset_save a on a.ul_geo_id = z.ul_rgn_geo_id 
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and z.ctry_geo_id = %s 
                    and ul_region_name != 'Country' """
    
    
    input_values = (division,geo_id,catg_cd,sub_catg_cd,geo_id)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    column_names = ["REGION_CD","REGION_NAME"]
    regionDF = pd.DataFrame(rows,columns=column_names)
    
    
    select_query = """select distinct z.ul_rgn_geo_id,z.ul_region_name 
                    from """+hana_db+""".ul_geo_hier z 
                    join """+hana_db+""".res_recordset_save a on a.ul_geo_id = z.ul_rgn_geo_id
                    join """+hana_db+""".ul_region_map c on c.ul_rgn_geo_id = z.ul_rgn_geo_id and c.off_rgn_geo_id != 0
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and z.ctry_geo_id =%s 
                    and ul_region_name != 'Country' """
    
    
    input_values = (catg_cd,sub_catg_cd,geo_id)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    column_names = ["REGION_CD","REGION_NAME"]
    resultDF = pd.DataFrame(rows,columns=column_names)
    if(len(resultDF)) > 0:
          regionDF = regionDF.append(resultDF)      
    
    #print(regionDF)
    
    # to get available formats  list 
    select_query = """select distinct c.fmt_cd,c.fmt_desc
                    from """+hana_db+""".format_type c
                    join """+hana_db+""".category_type d on c.catg_cd = d.catg_cd and d.div_cd = %s
                    where c.geo_id =%s
                    and c.catg_cd = %s
                    and c.subcat_cd = %s """
                        
    
    input_values = (division,geo_id,catg_cd,sub_catg_cd)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    column_names = ["FORMAT_CD","FORMAT_NAME"]
    formatDF = pd.DataFrame(rows,columns=column_names)
    #print(formatDF)
    
    responseDict1 = chennalDF.to_dict(orient='records')
    responseDict2 = regionDF.to_dict(orient='records')
    responseDict3 = formatDF.to_dict(orient='records')
    
    
    responseData ={"request_header":request_header,
                   "CHANNELS": responseDict1,
                   "REGIONS":responseDict2,
                   "FORMATS":responseDict3}
    
    json_object = json.dumps(responseData)
    return(json_object)
    
    

##################################################
### unit testing with different input values #####
##################################################

#input_values_ind = {"COUNTRY_NAME" : "73",
#                "DIVISION":"1",
#                "CATAGORY_NAME":"1",
#                "SUB_CATEGORY_NAME":"6",
#                "METRIC":"Volume",
#                "CHANNEL":"1",
#                "REGION":"0",
#                "FORMAT":"0",
#                "FORECAST_SCENARIO" : "2"}
#
#
#input_values = {"COUNTRY_NAME" : "142",
#                "DIVISION":"1",
#                "CATAGORY_NAME":"1",
#                "SUB_CATEGORY_NAME":"9",
#                "METRIC":"Volume",
#                "CHANNEL":"0",
#                "REGION":"0",
#                "FORMAT":"0",
#                "FORECAST_SCENARIO" : "1"}
#
#output1 = ul_db_input_validation(input_values)
#print(output1)
#   
#output2 = ul_db_input_validation_dropdown("142",1,1,9,"Volume")
#print(output2)

        
        