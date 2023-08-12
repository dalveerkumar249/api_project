#############################################################################################################
# This Program supplies Forecast data to the frontend. Contains One functions.
#############################################################################################################
# # 04 Aug 2021 - Ramesh - Originally coded
# 1.get forecast volume data based input parameters (geo_id,catg_cd,sub_catg_cd,metric) at Country level
# 2.get forecast Value data based input parameters (geo_id,catg_cd,sub_catg_cd,metric) at Country level
# 3.get forecast Value data based input parameters (geo_id,catg_cd,sub_catg_cd,metric) at Channel level
# 4.get forecast Value data based input parameters (geo_id,catg_cd,sub_catg_cd,metric) at Region level
# 5.get forecast Value data based input parameters (geo_id,catg_cd,sub_catg_cd,metric) at format level
#############################################################################################################



import pandas as pd
# from hdbcli import dbapi
# import numpy as np
import json
# from flask import jsonify
import sys
#from sap_hana_credentials import connection
#from flask_mysqldb import MySQL
from sap_hana_credentials import connection
import MySQLdb.cursors
import pymysql

#hana_db ='ULGROWTH20'



try:
    db_config_file = open("UL_DB_CONFIG.json")
    db_config_json = json.load(db_config_file)
    hana_db = db_config_json[0]['DB_NAME']
    db_config_file.close()
except:
    print("Error reading config file")
    sys.exit()
    
    
def ul_db_forecast_vol(geo_id,division,catg_cd,sub_catg_cd,metric):
    #73,1,3

    # credentials = pymysql.connect(
    #     host="localhost",
    #     user="root",
    #     password="Winter@123",
    #     database="ULGROWTH20",
    # )
    # conn=credentials
    # cursor = conn.cursor()

    #query = ("select * from RES_RECORDSET_SAVE")
    # cursor.execute(query)
    # reader = cursor.fetchall()   
    # return str(reader)

    
    """
    forcast volume at country level  
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 
        return values in json object : "MONTH_YEAR","VOLUME","DATA_TYPE","FORECAST_TYPE"

    """
    metric = metric.lower()
    if metric == "volume":
        select_query = """select  a.period_ending_date as MONTH_YEAR,
                    case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                    when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                    else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                    end as VOLUME,
                    JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE'),
                    c.SECTOR_SCENARIO_DESC
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                    join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    order by period_ending_date"""
        #return select_query
    
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
        
        #cursor_data = connection.cursor()
        #cursor_data.execute(select_query,input_values)
        #rows = cursor_data.fetchall()
        cursor = connection.cursor()
        cursor.execute(select_query,input_values)
        connection.commit()     
        rows = cursor.fetchall()   
        #print(str(select_query))
        #print(str(rows))
        #exit()
        #return str(rows)     




        #rows = cursor_data.fetchmany(5)
    
        request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
        column_names = ["MONTH_YEAR","VOLUME","DATA_TYPE","FORECAST_TYPE"]
        results_df = pd.DataFrame(rows,columns=column_names)
        #cursor_data.close()
        results_df["MONTH_YEAR"] = results_df["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        results_df["VOLUME"] = results_df["VOLUME"].astype(float)
        results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
        results_df["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
        responseDict = results_df.to_dict(orient='records')
        responseData ={"request_header":request_header,
                       "sales_volume": responseDict}
            
        json_object = json.dumps(responseData)  
        return json_object
        print(json_object)
    else:
        request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
        responseData ={"request_header":request_header,
                       "sales_value": []}
        json_object = json.dumps(responseData)  
        return json_object
    

def ul_db_forecast_val(parameters):

    """
    forcast value at country level,
    - 1 country is selected(passed in parameters), this function will return in LCU and EUR currenices
    - more than one countries are selected(passed in parameters), this function will return value in EVR and LCU will be zero
    Augs:
        Parameters(list of dict): geo_id,catg_cd, sub_catg_cd,metric are included in dict format
    
    Returns:
        json_object(json object): json data to online 
        return values in json object : "MONTH_YEAR","VALUE","DATA_TYPE","FORECAST_TYPE"  

    """
    
    # request_header = {}
    param_len = len(parameters)
    #return str(parameters)
    masterDF = pd.DataFrame()
    #These are values from frontend dropdowns and switches
    metric = parameters[0]['METRIC']
    #return metric
    metric = metric.lower() 
    if metric == "value":
        for i in range(0,param_len):
            geo_id = parameters[i]['COUNTRY_NAME']
            division = parameters[i]['DIVISION']
            catg_cd = parameters[i]['CATAGORY_NAME']
            sub_catg_cd = parameters[i]['SUB_CATEGORY_NAME']
            
        
            select_query = """select  a.period_ending_date as MONTH_YEAR,
                        case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        end as VALUE,
                        JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE'),
                        c.SECTOR_SCENARIO_DESC
                        from """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        and a.chnl_cd = 0
                        and a.fmt_cd = 0
                        order by period_ending_date"""
    
            input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
        
            cursor_data = connection.cursor()
            cursor_data.execute(select_query,input_values)
            rows = cursor_data.fetchall()
            #rows = cursor_data.fetchmany(5)
            print(str(rows))
            
            # to get exchange rate from ccy_exch_rate
            exrate_query = """select exch_rate from """+hana_db+""".ccy_exch_rate a
                        join """+hana_db+""".currency_type b on b.ccy_cd = a.from_ccy_cd and ctry_geo_id = %s  
                        where to_ccy_cd = 0"""
                            
            input_data =(geo_id)
            cursor_data.execute(exrate_query,input_data)
            rows1 = cursor_data.fetchmany(1)
            exch_rate = float(rows1[0][0]) 
            cursor_data.close()
        
            column_names = ["MONTH_YEAR","VALUE_LCU","DATA_TYPE","FORECAST_TYPE"]
            results_df = pd.DataFrame(rows,columns=column_names)
            results_df2= results_df.copy()
            results_df2["VALUE_LCU"] = results_df2["VALUE_LCU"].astype(float)
            results_df2.insert(2,"VALUE_EUR", results_df2["VALUE_LCU"] * exch_rate)
            cursor_data.close()
            masterDF = masterDF.append(results_df2)
                 
            # request_header =request_header.update({"COUNTRY_NAME":geo_id,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric})
        masterDF["MONTH_YEAR"] = masterDF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        
        
        
        # build dataframe based on input values 
        if param_len == 1 :
            finalDF = masterDF.copy()
            finalDF["FORECAST_TYPE"] = finalDF["FORECAST_TYPE"].str.replace(' ','_')
            finalDF["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
        else:
            finalDF1 = masterDF.groupby(["MONTH_YEAR","DATA_TYPE","FORECAST_TYPE"]).sum()
            finalDF =  finalDF1.reset_index()
            column_names = ["MONTH_YEAR","VALUE_LCU","VALUE_EUR","DATA_TYPE","FORECAST_TYPE"]
            finalDF = finalDF[column_names]
            finalDF["FORECAST_TYPE"] = finalDF["FORECAST_TYPE"].str.replace(' ','_')
            finalDF["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
#        
        #return finalDF
        responseDict = finalDF.to_dict(orient='records')
       
        responseData ={"request_header":parameters,
                       "sales_value": responseDict}
        
        json_object = json.dumps(responseData)  
        
        return json_object
    else:
        responseData ={"request_header":parameters,
                       "sales_value": []}
        json_object = json.dumps(responseData)  
        return json_object
        

def ul_get_generic_sales_forcast_data(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values):
    
    select_query ="""select """+selectq1+""", MONTH_YEAR, """+metric_name+""", RECORD_TYPE, SECTOR_SCENARIO_DESC
            FROM
            (select """+selectq2+""",
            """+selectmetric1+"""
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE,
            d.SECTOR_SCENARIO_DESC
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = e.catg_cd""" +joinq1+"""
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            and a.catg_cd = %s
            and a.subcat_cd = %s """+condition1+"""
            and a.SECTOR_SCENARIO_CD = 1
            group by """+grouby1+""" ,JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE'),
            d.SECTOR_SCENARIO_DESC
            UNION
            select """+selectq2+""",
            """+selectmetric2+"""
            JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE,
            d.SECTOR_SCENARIO_DESC
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = e.catg_cd""" +joinq1+"""
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            and a.catg_cd = %s
            and a.subcat_cd = %s """+condition1+"""
            and a.SECTOR_SCENARIO_CD = %s
            group by """+grouby1+""" ,JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE'),
            d.SECTOR_SCENARIO_DESC
            ) sub
            order by MONTH_YEAR,""" + selectq1
    #print(str(select_query))
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    return rows




def ul_get_generic_sales_forcast_data_quarter(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values):
   
    select_query ="""select """+selectq1+""", MONTH_YEAR, SUM("""+metric_name+""")
            FROM
            (select """+selectq2+""",
            """+selectmetric1+"""
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = e.catg_cd""" +joinq1+"""
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            """+condition1+"""
            and a.SECTOR_SCENARIO_CD = 1
            group by """+grouby1+""" 
            UNION
            select """+selectq2+""",
            """+selectmetric2+"""
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = e.catg_cd""" +joinq1+"""
            join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            """+condition1+"""
            and a.SECTOR_SCENARIO_CD = %s
            group by """+grouby1+""" 
            ) sub
            group by MONTH_YEAR,""" +selectq1 +"""
            order by MONTH_YEAR,""" + selectq1  
            
    
    #print(select_query)
    #exit()
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    return rows





def ul_db_channel_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric):    
    """
    forcast volume at channel level  
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 
        return values in json object : "CHANNEL","MONTH_YEAR","VOLUME","FORECAST_TYPE"
    """
    #return metric
    forecast_scenario = "1"
    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
    
    if metric == "volume":        
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME,"""
        
        
    
    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["CHANNEL","MONTH_YEAR","VOLUME","DATA_TYPE","FORECAST_TYPE"]
    
    selectq1 ="chnl_desc"
    selectq2 = "c.chnl_desc,a.period_ending_date as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name = 'Country'
                join """+hana_db+""".channel_type c on a.chnl_cd = c.chnl_cd and c.geo_id =%s"""
    
    grouby1 = "a.period_ending_date,c.chnl_desc"
    condition1 = "and a.chnl_cd != 0 and a.fmt_cd =0"
    results_df = pd.DataFrame(ul_get_generic_sales_forcast_data(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
    #return "ll"
    #return results_df
    # return empty data if data does not exit
    if len(results_df) < 1:
        
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)
        
    results_df["MONTH_YEAR"] = results_df["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    results_df["VOLUME"] = results_df["VOLUME"].astype(float)
    results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
    results_df["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
    responseDict = results_df.to_dict(orient='records')
    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
        
    json_object = json.dumps(responseData)  
    return json_object
    # print(json_object)

def ul_db_region_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric):   
    """
    forcast volume at region level , if any region ovelapping in sales it will look for ul region map table
    get the values based on mapping available from ul region map
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 
        return values in json object : "REGION","MONTH_YEAR","VOLUME","FORECAST_TYPE"
    """
    forecast_scenario = "1"
    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
    
    if metric == "volume":
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME,"""
       
 
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["REGION","MONTH_YEAR","VOLUME","DATA_TYPE","FORECAST_TYPE"]
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
    selectq1 ="ul_region_name"
    selectq2 = "z.ul_region_name,a.period_ending_date as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name != 'Country'"""
    
    grouby1 = "a.period_ending_date,z.ul_region_name"
    condition1 = "and a.chnl_cd = 0 and a.fmt_cd =0"
    
    results_df1 = pd.DataFrame(ul_get_generic_sales_forcast_data(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
    
    # check whether region within region mapping exist in ul_region_map
    check_query = """select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map a
                    join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s 
                    where ul_prmry_rgn_geo_id !=0 """
    
    input_values1 = (geo_id)
    cursor_data = connection.cursor()
    cursor_data.execute(check_query,input_values1)
    rows = cursor_data.fetchall()
    cursor_data.close()
    if len(rows) > 0:
        # to get overlapping region forcasted data 
        selectq1 ="ul_region_name"
        selectq2 = "z.ul_region_name,a.period_ending_date as MONTH_YEAR"
        joinq1 = """
                join """+hana_db+""".ul_region_map c on c.ul_prmry_rgn_geo_id = a.ul_geo_id and c.off_rgn_geo_id = 0
                join """+hana_db+""".ul_geo_hier z on z.ul_rgn_geo_id = c.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name != 'Country'"""
    
        grouby1 = "a.period_ending_date,z.ul_region_name"
        
        if str(geo_id)=='142' and str(sub_catg_cd) == '16':
           
            
            condition1 = "and z.ul_region_name like '%_IC' and a.chnl_cd = 0 and a.fmt_cd =0"
        
        elif str(geo_id)=='142':
            
            condition1 = "and z.ul_region_name not like '%_IC' and a.chnl_cd = 0 and a.fmt_cd =0"
        else:
            
            condition1 = "and a.chnl_cd = 0 and a.fmt_cd =0"
        
        results_df2 = pd.DataFrame(ul_get_generic_sales_forcast_data(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
        
        results_df = results_df1.append(results_df2,ignore_index=True)
        
        
    else:
        results_df = results_df1
    
     # return empty data if data does not exit
     
    if len(results_df) < 1:
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)
        
    
    results_df["MONTH_YEAR"] = results_df["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    results_df["VOLUME"] = results_df["VOLUME"].astype(float)
    results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
    request_header = {"COUNTRY_NAME":geo_id,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    results_df["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
    responseDict = results_df.to_dict(orient='records')
    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
    json_object = json.dumps(responseData)  
    return json_object
    #print(json_object)

def ul_db_format_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric):
    
    """
    forcast volume at format level
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 
        return values in json object : "FORMAT","MONTH_YEAR","VOLUME","FORECAST_TYPE"
    """
    
   
    forecast_scenario = "1"
   
    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE,"""
        
    
    if metric == "volume":
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME,"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME,"""
       
 
    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd,division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd,forecast_scenario)
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["FORMAT","MONTH_YEAR","VOLUME","DATA_TYPE","FORECAST_TYPE"]
    
    selectq1 ="fmt_desc"
    selectq2 = "c.fmt_desc,a.period_ending_date as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name = 'Country'
                join """+hana_db+""".format_type c on a.fmt_cd = c.fmt_cd and c.geo_id =%s and c.catg_cd = %s and c.subcat_cd = %s"""
    
    grouby1 = "a.period_ending_date,c.fmt_desc"
    condition1 = "and a.chnl_cd = 0 and a.fmt_cd != 0"
    
    results_df = pd.DataFrame(ul_get_generic_sales_forcast_data(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
    #print(str(results_df))
    #exit()
    # returnrn  empty data if data does not exit
    if len(results_df) < 1:
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)

  
    results_df["MONTH_YEAR"] = results_df["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    results_df["VOLUME"] = results_df["VOLUME"].astype(float)
    results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
    results_df["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')
    responseDict = results_df.to_dict(orient='records')

    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
        
    json_object = json.dumps(responseData)  
    return json_object
    # print(json_object)

#data_output = ul_db_format_wise_sales(73,2,4,1,"Value")
#data_output = ul_db_format_wise_sales(142,1,1,9,"Value")
#print(data_output)

#input_valus = [{"COUNTRY_NAME" : "142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME": "9","FORECAST_SCENARIO": "1","METRIC" : "VALUE"},
#               {"COUNTRY_NAME" : "142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME": "10","FORECAST_SCENARIO": "1","METRIC" : "VALUE"}]
#
#data_output = ul_db_forecast_val(input_valus)
#print(data_output)    