# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 15:45:49 2021

@author: Rameshkumar
"""


import pandas as pd
# from hdbcli import dbapi
# import numpy as np
import json
# from flask import jsonify
import sys

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
    
def ul_db_treemap(geo_id,division,catg_cd,sub_catg_cd,metric):
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 
        return values in json object : "SEGMENT","SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","SALE_VALUE"

    """
   
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
     
    # get channel level data 
    select_query1 = """select c.chnl_desc, year(a.period_ending_date) as YEAR,
                    sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                    when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                    else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                    end) as VOLUME,
                    sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                    when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                    else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                    end) as VALUE,
                    d.SECTOR_SCENARIO_DESC
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                    join """+hana_db+""".channel_type c on a.chnl_cd = c.chnl_cd and c.geo_id =%s
                    join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and a.chnl_cd != 0
                    and a.fmt_cd =0
                    and a.SECTOR_SCENARIO_CD = 1
                    group by year(a.period_ending_date),c.chnl_desc ,
                    d.SECTOR_SCENARIO_DESC
                    order by year(a.period_ending_date),c.chnl_desc"""

    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query1,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    column_names = ["SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","SALE_VALUE","FORECAST_TYPE"]
    results_df1 = pd.DataFrame(rows,columns=column_names)
    results_df1.insert(0,"SEGMENT", "CHANNEL")
    
    
    #to get region level data 
    select_query2 = """select z.ul_region_name, year(a.period_ending_date) as year,
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                end) as VOLUME,
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                end) as VALUE,
                d.SECTOR_SCENARIO_DESC
                from """+hana_db+""".res_recordset_save a
                join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'                
                join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                where a.catg_cd = %s
                and a.subcat_cd = %s
                and a.chnl_cd = 0
                and a.fmt_cd =0
                and a.SECTOR_SCENARIO_CD = 1
                group by year(a.period_ending_date), z.ul_region_name,
                d.SECTOR_SCENARIO_DESC
                order by year(a.period_ending_date),z.ul_region_name"""

    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    cursor_data.execute(select_query2,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    column_names = ["SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","SALE_VALUE","FORECAST_TYPE"]
    results_df2 = pd.DataFrame(rows,columns=column_names)
    results_df2.insert(0,"SEGMENT", "REGION")
    
     # check whether region within region mapping exist in ul_region_map
    check_query = """select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map a
                    join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s 
                    where ul_prmry_rgn_geo_id !=0 """
    
    input_values = (geo_id)
    cursor_data.execute(check_query,input_values)
    rows = cursor_data.fetchall()
    if len(rows) > 0:
        # special case for indonesia IC
        if str(geo_id)=='142' and str(sub_catg_cd) == '16':
           
            # to get overlapping regions data 
            select_query3 = """select z.ul_region_name, year(a.period_ending_date) as YEAR,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                        end) as VOLUME,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        end) as VALUE,
                        d.SECTOR_SCENARIO_DESC
                        from """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                        join """+hana_db+""".ul_region_map c on c.ul_prmry_rgn_geo_id = a.ul_geo_id and c.off_rgn_geo_id = 0 
                        join """+hana_db+""".ul_geo_hier z on z.ul_rgn_geo_id = c.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        and a.chnl_cd = 0
                        and a.fmt_cd =0
                        and a.SECTOR_SCENARIO_CD = 1
                        and z.ul_region_name like '%_IC'
                        group by year(a.period_ending_date), d.SECTOR_SCENARIO_DESC,
                        z.ul_region_name
                        order by year(a.period_ending_date),
                        z.ul_region_name"""

        
        #different select query for all other regions
        elif str(geo_id)=='142':
           
            # to get overlapping regions data 
            select_query3 = """select z.ul_region_name, year(a.period_ending_date) as YEAR,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                        end) as VOLUME,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        end) as VALUE,
                        d.SECTOR_SCENARIO_DESC
                        from """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                        join """+hana_db+""".ul_region_map c on c.ul_prmry_rgn_geo_id = a.ul_geo_id and c.off_rgn_geo_id = 0 
                        join """+hana_db+""".ul_geo_hier z on z.ul_rgn_geo_id = c.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        and a.chnl_cd = 0
                        and a.fmt_cd =0
                        and a.SECTOR_SCENARIO_CD = 1
                        and z.ul_region_name not like '%_IC'
                        group by year(a.period_ending_date), d.SECTOR_SCENARIO_DESC,
                        z.ul_region_name
                        order by year(a.period_ending_date),
                        z.ul_region_name"""
        
        
        else:
             # to get overlapping regions data             
            select_query3 = """select z.ul_region_name, year(a.period_ending_date) as YEAR,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                        end) as VOLUME,
                        sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                        when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                        end) as VALUE,
                        d.SECTOR_SCENARIO_DESC
                        from """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                        join """+hana_db+""".ul_region_map c on c.ul_prmry_rgn_geo_id = a.ul_geo_id and c.off_rgn_geo_id = 0 
                        join """+hana_db+""".ul_geo_hier z on z.ul_rgn_geo_id = c.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        and a.chnl_cd = 0
                        and a.fmt_cd =0
                        and a.SECTOR_SCENARIO_CD = 1
                        group by year(a.period_ending_date), d.SECTOR_SCENARIO_DESC,
                        z.ul_region_name
                        order by year(a.period_ending_date),
                        z.ul_region_name"""
        
   
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
        
        
        cursor_data.execute(select_query3,input_values)
        rows = cursor_data.fetchall()
        #rows = cursor_data.fetchmany(5)
        column_names = ["SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","SALE_VALUE","FORECAST_TYPE"]
        results_df2_sub = pd.DataFrame(rows,columns=column_names)
        results_df2_sub.insert(0,"SEGMENT", "REGION")
        results_df2 = results_df2.append(results_df2_sub,ignore_index=True)
    
    
     #to get format level data 
    select_query4 = """select  c.fmt_desc,year(a.period_ending_date) as YEAR,
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') 
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)
                end) as VOLUME,
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                end) as VALUE,
                d.SECTOR_SCENARIO_DESC
                from """+hana_db+""".res_recordset_save a
                join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd and e.div_cd = %s
                join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                join """+hana_db+""".format_type c on a.fmt_cd = c.fmt_cd and c.geo_id =%s
                join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                and c.catg_cd = %s and c.subcat_cd = %s
                where a.catg_cd = %s
                and a.subcat_cd = %s
                and a.chnl_cd = 0
                and a.fmt_cd != 0
                and a.SECTOR_SCENARIO_CD = 1
                group by year(a.period_ending_date),c.fmt_desc,d.SECTOR_SCENARIO_DESC
                order by year(a.period_ending_date),c.fmt_desc"""

    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd)
    
    cursor_data.execute(select_query4,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    column_names = ["SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","SALE_VALUE","FORECAST_TYPE"]
    results_df3 = pd.DataFrame(rows,columns=column_names)
    results_df3.insert(0,"SEGMENT", "FORMAT")
    frames = [results_df1,results_df2,results_df3]
    results_df = pd.concat(frames)
    
    
    select_units = """ select vol_unit_desc from """+hana_db+""".vol_unit_type a
                    join """+hana_db+""".subcat_type b on a.vol_unit_cd = b.vol_unit_cd and  
                    geo_id= %s and CATG_CD = %s and SUBCAT_CD = %s """
    
    input_values = (geo_id,catg_cd,sub_catg_cd)
    cursor_data = connection.cursor()
    cursor_data.execute(select_units,input_values)
    rows = cursor_data.fetchmany(1)
    cursor_data.close()
    value_unit = rows[0][0]
        
    
    metric = metric.lower()
    
    if metric == "value":
        results_df["SALE_VOLUME"] = results_df["SALE_VOLUME"].astype(float)
        results_df["SALE_VALUE"] = results_df["SALE_VALUE"].astype(float)
        results_df["SALE_VALUE"] =  round(results_df["SALE_VALUE"]/1000000,2)
        results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
        results_df = results_df[["SEGMENT","SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VALUE","FORECAST_TYPE"]]
        responseDict = results_df.to_dict(orient='records')
        responseData ={"request_header":request_header,
                   "sales_volume": responseDict,
                   "metric":"Value(Millions in LCU)"}
        
        json_object = json.dumps(responseData)  
        #print(json_object)
        return json_object
       
    
    if metric == "volume":
              
        results_df["SALE_VOLUME"] = results_df["SALE_VOLUME"].astype(float)
        results_df["SALE_VALUE"] = results_df["SALE_VALUE"].astype(float)
        results_df["SALE_VOLUME"] =  round(results_df["SALE_VOLUME"]/1000000,2)
        results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
        results_df = results_df[["SEGMENT","SEGMENT_SLICE","SEGMENT_SLICE_YEAR","SALE_VOLUME","FORECAST_TYPE"]]
        responseDict = results_df.to_dict(orient='records')
        responseData ={"request_header":request_header,
                       "sales_volume": responseDict,
                       "metric":"Volume (Millions in "+str(value_unit)+")"}
            
        json_object = json.dumps(responseData)  
        #print(json_object)
        return json_object
    
    
    
    
#treemap_data = ul_db_treemap(142,1,8,1,"Volume")
#print(treemap_data)