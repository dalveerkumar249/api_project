# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 15:55:36 2021

@author: Rameshkumar
"""

import pandas as pd
# from hdbcli import dbapi
# import numpy as np
import json
# from flask import jsonify
import ul_forecast
import numpy as np
import sys

#hana_db ='ULGROWTH20'

try:
    db_config_file = open("UL_DB_CONFIG.json")
    db_config_json = json.load(db_config_file)
    hana_db = db_config_json[0]['DB_NAME']
    db_config_file.close()
except:
    print("Error reading config file")
    sys.exit()
    

from sap_hana_credentials import connection
from ul_generic_ind_contribution import ul_db_generic_ind_contribution

np.seterr(divide='ignore', invalid='ignore')


def grow_rate_calc(yearDF,compyearDF,key1,key2,key3,metric,conversion):
    
    sales_volume = {key1:[],key2:[],key3:[]}
    
    try:
        sales_volume[key1].append("YEAR")
        sales_volume[key2].append(round((yearDF[metric].sum())/conversion,2))
        sales_volume[key3].append(round(((yearDF[metric].sum() - compyearDF[metric].sum())/compyearDF[metric].sum())*100,2))
    except:
        sales_volume[key1].append("YEAR")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
        
    try:
        yearHY1 = yearDF.loc[yearDF['MONTH']=="Q1",metric].values[0] + yearDF.loc[yearDF['MONTH']=="Q2",metric].values[0]
        compyearHY1 = compyearDF.loc[compyearDF['MONTH']=="Q1",metric].values[0] + compyearDF.loc[compyearDF['MONTH']=="Q2",metric].values[0]
        sales_volume[key1].append("HY1")
        sales_volume[key2].append(round(yearHY1/conversion,2))
        sales_volume[key3].append(round(((yearHY1 - compyearHY1)/compyearHY1)*100,2))

    except:
        sales_volume[key1].append("HY1")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
    
    try:
        yearHY2 = yearDF.loc[yearDF['MONTH']=="Q3",metric].values[0] + yearDF.loc[yearDF['MONTH']=="Q4",metric].values[0]
        compyearHY2 = compyearDF.loc[compyearDF['MONTH']=="Q3",metric].values[0] + compyearDF.loc[compyearDF['MONTH']=="Q4",metric].values[0]

        sales_volume[key1].append("HY2")
        sales_volume[key2].append(round(yearHY2/conversion,2))
        sales_volume[key3].append(round(((yearHY2 - compyearHY2)/compyearHY2)*100,2))

    except:
        sales_volume[key1].append("HY2")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
        
       
    try:
        yearQ1 = yearDF.loc[yearDF['MONTH']=="Q1",metric].values[0]
        compyearQ1 = compyearDF.loc[compyearDF['MONTH']=="Q1",metric].values[0]
        sales_volume[key1].append("QTR1")
        sales_volume[key2].append(round(yearQ1/conversion,2))
        sales_volume[key3].append(round(((yearQ1 - compyearQ1)/compyearQ1)*100,2))
    except:
        sales_volume[key1].append("QTR1")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
    
    try:
        yearQ2 = yearDF.loc[yearDF['MONTH']=="Q2",metric].values[0]
        compyearQ2 = compyearDF.loc[compyearDF['MONTH']=="Q2",metric].values[0]
        sales_volume[key1].append("QTR2")
        sales_volume[key2].append(round(yearQ2/conversion,2))
        sales_volume[key3].append(round(((yearQ2 - compyearQ2)/compyearQ2)*100,2))
    except:
        sales_volume[key1].append("QTR2")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
    
    try:
        yearQ3 = yearDF.loc[yearDF['MONTH']=="Q3",metric].values[0]
        compyearQ3 = compyearDF.loc[compyearDF['MONTH']=="Q3",metric].values[0]
        sales_volume[key1].append("QTR3")
        sales_volume[key2].append(round(yearQ3/conversion,2))
        sales_volume[key3].append(round(((yearQ3 - compyearQ3)/compyearQ3)*100,2))
    except:
        sales_volume[key1].append("QTR3")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
    
    try:
        yearQ4 = yearDF.loc[yearDF['MONTH']=="Q4",metric].values[0]
        compyearQ4 = compyearDF.loc[compyearDF['MONTH']=="Q4",metric].values[0]
        sales_volume[key1].append("QTR4")
        sales_volume[key2].append(round(yearQ4/conversion,2))
        sales_volume[key3].append(round(((yearQ4 - compyearQ4)/compyearQ4)*100,2))
    except:
        sales_volume[key1].append("QTR4")
        sales_volume[key2].append(0)
        sales_volume[key3].append(0)
        
    return sales_volume


def data_check(input_values1,input_values2):
    
    check_query1 = """ select PERIOD_ENDING_DATE  
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s
                    join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    and year(a.PERIOD_ENDING_DATE) = %s
                    and Month(a.PERIOD_ENDING_DATE) = 1 """
    
    cursor_data = connection.cursor()
    cursor_data.execute(check_query1,input_values1)
    rows1 = cursor_data.fetchall()
    
    #print(rows1)
    check_query2 = """ select PERIOD_ENDING_DATE 
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s
                    join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    and year(a.PERIOD_ENDING_DATE) = %s
                    and Month(a.PERIOD_ENDING_DATE) = 1 """
    
    cursor_data = connection.cursor()
    cursor_data.execute(check_query2,input_values2)
    rows2 = cursor_data.fetchall()
    cursor_data.close()
    
    if len(rows1) > 0 and len(rows2) > 0:
        return True
    else:
        return False
        
    
def ul_db_growth_rate_get_data(selectq1,selectq2,selectq3,input_values):
     
    select_query ="""select MONTH_YEAR, SUM(""" +selectq1+""")
            FROM
            (select CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
            """+selectq2+"""
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            AND a.SECTOR_SCENARIO_CD = 1
            and year(a.period_ending_date) in (%s, %s)
            group by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
            union
            select CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
             """+selectq3+"""
            from """+hana_db+""".res_recordset_save a
            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s
            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
            join """+hana_db+""".sector_scenario_type c on a.SECTOR_SCENARIO_CD = c.SECTOR_SCENARIO_CD
            where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
            and a.catg_cd = %s
            and a.subcat_cd = %s
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            AND a.SECTOR_SCENARIO_CD = %s
            and year(a.period_ending_date) in (%s, %s)
            group by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
            ) sub
            group by MONTH_YEAR
            order by MONTH_YEAR"""

    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #print("Hello")
    #print(str(rows))
    #exit()
    return rows



def ul_db_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd, year,comp_year, forecast_scenario, metric):
   
  
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 

    """
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                          "YEAR": year,"COMPARISON_YEAR":comp_year , "FORECAST_SCENARIO": forecast_scenario,
                      "METRIC":metric}
   
    input_values1 = (geo_id,geo_id,catg_cd,sub_catg_cd,year)
    input_values2 = (geo_id,geo_id,catg_cd,sub_catg_cd,comp_year)
    #return "one1"
    if data_check(input_values1,input_values2):     
        
        #exit()
        selectq1 ="VOLUME"
        selectq2 = """sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as VOLUME"""

        selectq3 = """sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as VOLUME """

                           
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,year,comp_year,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,year,comp_year)
        column_names = ["MONTH_YEAR","VOLUME"]
        
        results_df1 = pd.DataFrame(ul_db_growth_rate_get_data(selectq1,selectq2,selectq3,input_values),columns=column_names)
        # print("one2")
        # print(str(results_df1))
        # exit()
        results_df1[["YEAR","MONTH"]] = results_df1["MONTH_YEAR"].str.split("-",expand=True)
        results_df1["VOLUME"] = results_df1["VOLUME"].astype(float)
        yearDF = results_df1[results_df1["YEAR"]==str(year)]
        compyearDF = results_df1[results_df1["YEAR"]==str(comp_year)]
        # volumne converting into million
        resultDict = grow_rate_calc(yearDF,compyearDF,"PERIOD_SLICE","VOLUME","VOLUME_GROWTH","VOLUME",1000000000)
        resultDF = pd.DataFrame(resultDict)
       

        select_units = """ select vol_unit_desc from """+hana_db+""".vol_unit_type a
                        join """+hana_db+""".subcat_type b on a.vol_unit_cd = b.vol_unit_cd and  
                        geo_id= %s and CATG_CD = %s and SUBCAT_CD = %s """
        
        input_values = (geo_id,catg_cd,sub_catg_cd)
        cursor_data = connection.cursor()
        cursor_data.execute(select_units,input_values)
        rows = cursor_data.fetchmany(1)
        cursor_data.close()
        value_unit = rows[0][0]
        
        responseDict = resultDF.to_dict(orient='records')
        responseData ={"request_header":request_header,
                   "sales_volume": responseDict,
                   "units":value_unit}
        
        json_object = json.dumps(responseData)  
        #print(json_object)
        return json_object
        
    else:
        #return "one2"
        responseData ={"request_header":request_header,
                   "sales_volume": "Data does not exist"}
        json_object = json.dumps(responseData)  
        #print(json_object)
        return json_object
        



def ul_db_growth_rate_val(parameters):
    
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 

    """



    masterDF = pd.DataFrame()
    input_check = False
    
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    year = parameters['YEAR']
    comp_year =  parameters['COMPARISON_YEAR']
    forecast_scenario = parameters['FORECAST_SCENARIO']
    metric = parameters['METRIC']
    
#        request_header = {"COUNTRY_NAME":geo_id,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
#                              "YEAR": year,"COMPARISON_YEAR":comp_year , "FORECAST_SCENARIO": forecast_secenario,
#                          "METRIC":metric}
#        
    input_values1 = (geo_id,geo_id,catg_cd,sub_catg_cd,year)
    input_values2 = (geo_id,geo_id,catg_cd,sub_catg_cd,comp_year)
    
    
    if data_check(input_values1,input_values2):
        input_check = True
        
        selectq1 ="VALUE"
        selectq2 = """sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) *
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL')as decimal)) as VALUE"""

        selectq3 = """sum(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) *
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL')as decimal)) as VALUE """

                       
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,year,comp_year,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario,year,comp_year)
        column_names = ["MONTH_YEAR","VALUE"]
        results_df1 = pd.DataFrame(ul_db_growth_rate_get_data(selectq1,selectq2,selectq3,input_values),columns=column_names)
        # to get exchange rate from ccy_exch_rate
        exrate_query = """select exch_rate from """+hana_db+""".ccy_exch_rate a
                    join """+hana_db+""".currency_type b on b.ccy_cd = a.from_ccy_cd and ctry_geo_id = %s  
                    where to_ccy_cd = 0"""
                        
        input_data =(geo_id)
        cursor_data = connection.cursor()
        cursor_data.execute(exrate_query,input_data)
        rows1 = cursor_data.fetchmany(1)
        exch_rate = float(rows1[0][0]) 
        cursor_data.close()
        results_df2= results_df1.copy()
        results_df2["VALUE"] = results_df2["VALUE"].astype(float)
        results_df2.insert(2,"VALUE_EUR", results_df2["VALUE"] * exch_rate)
        masterDF = masterDF.append(results_df2)

    
    if input_check == False:
        responseData ={"request_header":parameters,
        "sales_volume": []}
    else:
        
    
    # check the number of countries selected

        masterDF[["YEAR","MONTH"]] = masterDF["MONTH_YEAR"].str.split("-",expand=True)
        yearDF = masterDF[masterDF["YEAR"]==str(year)]
        compyearDF = masterDF[masterDF["YEAR"]==str(comp_year)]
        # Value converting into billion
        resultDict1 = grow_rate_calc(yearDF,compyearDF,"PERIOD_SLICE","VALUE","VALUE_GROWTH","VALUE",1000000000)
        resultDict2 = grow_rate_calc(yearDF,compyearDF,"PERIOD_SLICE","VALUE_EUR","VALUE_GROWTH","VALUE_EUR",1000000000)
        resultDF1 = pd.DataFrame(resultDict1)
        resultDF2 = pd.DataFrame(resultDict2)
        resultDF1.insert(2,"VALUE_LCU", resultDF1["VALUE"])
        resultDF1.insert(3,"VALUE_EUR", resultDF2["VALUE_EUR"])
        
        responseDict = resultDF1.to_dict(orient='records')
        responseData ={"request_header":parameters,
                   "sales_volume": responseDict}

    
    json_object = json.dumps(responseData)  
    #print(json_object)
    return json_object
#    

  

#input_valus = {"COUNTRY_NAME" : "142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME": "9","YEAR":"2021","COMPARISON_YEAR":"2020","FORECAST_SCENARIO": "1","METRIC" : "VALUE"}
##
##
##input_valus = [{"COUNTRY_NAME" : "142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME": "9","YEAR":"2021","COMPARISON_YEAR":"2020","FORECAST_SCENARIO": "1","METRIC" : "VALUE"}]
##
##
#DF1 = ul_db_growth_rate_val(input_valus)
#print(DF1)


def ul_generic_growth_rate(tempDF,segment):
       
    tempDF["VOLUME"] = tempDF["VOLUME"].astype(float)
    #pivotDF = resultsDF.pivot(index='YEAR_QTR',columns='CHANNEL',values='VOLUME').reset_index()
    tempDF[["YEAR","MONTH"]] = tempDF["YEAR_QTR"].str.split("-",expand=True)
    tempDF["YEAR"] = tempDF["YEAR"].astype(int)
    # print(resultsDF)
    masterDF = pd.DataFrame()
    starting_year = min(tempDF[tempDF['MONTH']=='Q1']['YEAR'])+1
    year_list = list(tempDF[tempDF['MONTH']=='Q1']['YEAR'].unique())
    type_list = list(tempDF[segment].unique())
    #print(str(year_list))
    #exit()
    for item in type_list:
        for year in year_list:
            if year >= starting_year:
                yearDF = tempDF[(tempDF["YEAR"]== year) & (tempDF[segment]==item)][['MONTH','VOLUME']]
                compyearDF = tempDF[(tempDF["YEAR"]== year-1) & (tempDF[segment]==item)][['MONTH','VOLUME']]
                resultDict = grow_rate_calc(yearDF,compyearDF,"PERIOD_TYPE","VOLUME","VOLUME_GROWTH","VOLUME",1)
                resultDF1 = pd.DataFrame(resultDict)
                resultDF1['YEAR'] =year
                resultDF1[segment] = item
                masterDF = masterDF.append(resultDF1)

    return masterDF



def ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,metric,output):
    
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 

    """
    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
        
    
    if metric == "volume":
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME"""


    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}

    column_names = ["CHANNEL","YEAR_QTR","VOLUME"]
    
    selectq1 ="chnl_desc"
    selectq2 = "c.chnl_desc,CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id =%s and  z.ul_region_name = 'Country'
                join """+hana_db+""".channel_type c on a.chnl_cd = c.chnl_cd and c.geo_id =%s """
    
    grouby1 = "CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),c.chnl_desc"
    condition1 = "and a.chnl_cd != 0 and a.fmt_cd =0"
    
    
    resultsDF = pd.DataFrame(ul_forecast.ul_get_generic_sales_forcast_data_quarter(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
    print(resultsDF)
    
    
    if resultsDF.shape[0] < 1:
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)
        

    
    if output =="DF":
        masterDF = ul_generic_growth_rate(resultsDF,"CHANNEL")
        return masterDF
    
    if output =="growth":
        masterDF = ul_generic_growth_rate(resultsDF,"CHANNEL")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    elif output =="ind":
        masterDF = ul_db_generic_ind_contribution(resultsDF,"CHANNEL")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    else:
        masterDF = ul_generic_growth_rate(resultsDF,"CHANNEL")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME_GROWTH').reset_index()
        pivotDF = pivotDF.loc[pivotDF['PERIOD_TYPE'].isin(['QTR1','QTR2','QTR3','QTR4'])]
        
    
    replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
    pivotDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
    
    #pivotDF.to_csv("test3.csv")
    responseDict = pivotDF.to_dict(orient='records')
    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
        
    json_object = json.dumps(responseData)  
    #print(json_object)
    return json_object
        
#print(ul_db_channel_growth_rate_vol(142,1,9,1, "Volume","table"))


def ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,metric,output):
   
    """
    region level growth rate to online 
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 

    """

    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE """
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE """
        
    
    if metric == "volume":
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME """
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME """


    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["REGION","YEAR_QTR","VOLUME"]
   
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
    selectq1 ="ul_region_name"
    selectq2 = "z.ul_region_name,CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name != 'Country'
                """
    
    grouby1 = "CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),z.ul_region_name"
    condition1 = "and a.chnl_cd = 0 and a.fmt_cd =0"
    
    results_df1 = pd.DataFrame(ul_forecast.ul_get_generic_sales_forcast_data_quarter(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
  
    resultsDF1 = results_df1[["REGION","YEAR_QTR","VOLUME"]].copy()
    
    check_query = """select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map a
                    join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s 
                    where ul_prmry_rgn_geo_id !=0 """
    
    input_values1 = (geo_id)
    cursor_data = connection.cursor()
    cursor_data.execute(check_query,input_values1)
    rows = cursor_data.fetchall()
    
         
    if len(rows) > 0:
        # to get overlapping region forcasted data 
        selectq1 ="ul_region_name"
        selectq2 = "z.ul_region_name,CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR"
        joinq1 = """
                join """+hana_db+""".ul_region_map c on c.ul_prmry_rgn_geo_id = a.ul_geo_id and c.off_rgn_geo_id = 0
                join """+hana_db+""".ul_geo_hier z on z.ul_rgn_geo_id = c.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name != 'Country'"""
    
        grouby1 = "CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),z.ul_region_name"
        
        # adding additional check to filter only IC region
       
        if str(geo_id)=='142' and str(sub_catg_cd) == '16':
            
            condition1 = "and z.ul_region_name like '%_IC' and a.chnl_cd = 0 and a.fmt_cd =0"
        
        elif str(geo_id)=='142' :
            
            condition1 = "and z.ul_region_name not like '%_IC' and a.chnl_cd = 0 and a.fmt_cd =0"
        
        else:
            condition1 = "and a.chnl_cd = 0 and a.fmt_cd =0"
    
        resultsdf2 = pd.DataFrame(ul_forecast.ul_get_generic_sales_forcast_data_quarter(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
        results_df2 = resultsdf2[["REGION","YEAR_QTR","VOLUME"]].copy()
        resultsDF = resultsDF1.append(results_df2,ignore_index=True)
    else:
        resultsDF = resultsDF1
    
    
   
    if resultsDF.shape[0] < 1:
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)
    
    
    if output =="DF":
        masterDF = ul_generic_growth_rate(resultsDF,"REGION")
        return masterDF
    
    if output =="growth":
        masterDF = ul_generic_growth_rate(resultsDF,"REGION")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    elif output =="ind":
        masterDF = ul_db_generic_ind_contribution(resultsDF,"REGION")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    else:
        masterDF = ul_generic_growth_rate(resultsDF,"REGION")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME_GROWTH').reset_index()
        pivotDF = pivotDF.loc[pivotDF['PERIOD_TYPE'].isin(['QTR1','QTR2','QTR3','QTR4'])]
        
    
    
    replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
    pivotDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
    
    #pivotDF.to_csv("test3.csv")
    responseDict = pivotDF.to_dict(orient='records')
    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
        
    json_object = json.dumps(responseData)  
    #print(json_object)
    return json_object

def ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,metric,output):
   
    """
    format level growth rate volume
    
    Augs:
        geo_id(int):country id
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        metric(str): it has to be volume
               
    Returns:
        json_object(json object): json data to online 

    """
    metric = metric.lower() 
    if metric == "value":
        
        metric_name = "SALES_VALUE"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
	                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
	                      cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
        
    
    if metric == "volume":
        metric_name = "SALES_VOLUME"
        selectmetric1 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME"""
        
        selectmetric2 = """SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME"""


    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
                      "FORECAST_SCENARIO": forecast_scenario,"METRIC":metric}
    
    input_values = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd,division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd,forecast_scenario)
    
    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["FORMAT","YEAR_QTR","VOLUME"]
    
    selectq1 ="fmt_desc"
    selectq2 = "c.fmt_desc,CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR"
    joinq1 = """
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and  z.ul_region_name = 'Country'
                join """+hana_db+""".format_type c on a.fmt_cd = c.fmt_cd and c.geo_id =%s and c.catg_cd = %s and c.subcat_cd = %s"""
    
    grouby1 = "CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),c.fmt_desc"
    condition1 = "and a.chnl_cd = 0 and a.fmt_cd != 0"
    
    results_df = pd.DataFrame(ul_forecast.ul_get_generic_sales_forcast_data_quarter(metric_name,selectmetric1,selectmetric2,selectq1,selectq2,joinq1,condition1,grouby1,input_values),columns=column_names)
    
    if results_df.shape[0] < 1:
        responseData ={"request_header":request_header,
                       "sales_volume": []}
    
        json_object = json.dumps(responseData)
        return(json_object)
        
    resultsDF = results_df[["FORMAT","YEAR_QTR","VOLUME"]].copy()
  
    if output =="DF":
        masterDF = ul_generic_growth_rate(resultsDF,"FORMAT")
        return masterDF
 
    if output =="growth":
        masterDF = ul_generic_growth_rate(resultsDF,"FORMAT")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='FORMAT',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    elif output =="ind":
        masterDF = ul_db_generic_ind_contribution(resultsDF,"FORMAT")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='FORMAT',values='VOLUME_GROWTH').reset_index()
        #pivotDF.insert(2,'Overall Contribution', round(pivotDF.iloc[:,2:].sum(axis=1),2))
    
    else:
        masterDF = ul_generic_growth_rate(resultsDF,"FORMAT")
        pivotDF = pd.pivot_table(masterDF,index=['YEAR','PERIOD_TYPE'],columns='FORMAT',values='VOLUME_GROWTH').reset_index()
        pivotDF = pivotDF.loc[pivotDF['PERIOD_TYPE'].isin(['QTR1','QTR2','QTR3','QTR4'])]
        
    
    
    replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
    pivotDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
    
    #pivotDF.to_csv("test3.csv")
    responseDict = pivotDF.to_dict(orient='records')
    responseData ={"request_header":request_header,
                   "sales_volume": responseDict}
        
    json_object = json.dumps(responseData)  
    #print(json_object)
    return json_object




#formatdata = ul_db_region_growth_rate_vol(142,3,11,16,1,"Value","Table")
#print(formatdata)
#    














