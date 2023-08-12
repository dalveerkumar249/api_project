# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import datetime
import json
from sap_hana_credentials import connection
import sys

#hana_db ='ULGROWTH20'

import ul_growth_rate

# total level simulation programs
import simulation_function_Indonesia
import Simulation_Function_India_Total
import Simulation_function_UK
import Simulation_function_SA


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
    sys.exit()
    
def get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values):
    
    select_query ="""select MONTH_YEAR, """ +selectq1+""" ,RECORD_TYPE
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
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            union
            select a.period_ending_date as MONTH_YEAR,
             """+selectq4+""",
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
            and a.chnl_cd = 0
            and a.fmt_cd = 0
            ) sub
            order by MONTH_YEAR"""
            
    

    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    #print(select_query)
    ##print(str(rows))
    #exit()
    return rows




def get_input_variables_data(selectq,selectq1,inp_date,input_values):
    
    # update query to get data based on forecast scenario and COVID_FLAG
    # select_query = """  select MONTH_YEAR ,"""+selectq+"""
    #                     from (
    #                     SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
    #                     """+selectq1+"""
    #                     FROM """+hana_db+""".res_recordset_save a
    #                     JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
    #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
    #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
    #                     join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
    #                     where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
    #                     AND a.CATG_CD = %s
    #                     AND a.SUBCAT_CD = %s
    #                     AND a.SECTOR_SCENARIO_CD = 1 
    #                     and a.CHNL_CD = 0
    #                     AND a.FMT_CD = 0
    #                     and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
	# 		            group by quarter(a.PERIOD_ENDING_DATE)
    #                     UNION
    #                     SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
    #                     """+selectq1+"""
    #                     FROM """+hana_db+""".res_recordset_save a
    #                     JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
    #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
    #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
    #                     join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
    #                     where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
    #                     AND a.CATG_CD = %s
    #                     AND a.SUBCAT_CD = %s
    #                     AND a.SECTOR_SCENARIO_CD = %s 
    #                     and a.CHNL_CD = 0
    #                     AND a.FMT_CD = 0
    #                     and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
	# 		            group by quarter(a.PERIOD_ENDING_DATE)
    #                     ) sub
    #                     order by MONTH_YEAR;
    #                     """


    select_query = """  select MONTH_YEAR ,"""+selectq+"""
                        from (
                        SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
                        """+selectq1+"""
                        FROM """+hana_db+""".res_recordset_save a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = 1 
                        and a.CHNL_CD = 0
                        AND a.FMT_CD = 0
                        and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
			            group by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                        UNION
                        SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
                        """+selectq1+"""
                        FROM """+hana_db+""".res_recordset_save a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.CHNL_CD = 0
                        AND a.FMT_CD = 0
                        and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
			            group by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                        ) sub
                        order by MONTH_YEAR;
                        """
    
    cursor_data = connection.cursor()
    print(select_query)
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


 
def get_input_variables_data_perf(selectq,selectq1,inp_date,input_values):
    
    # update query to get data based on forecast scenario and COVID_FLAG
    # select_query = """  SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
    #                     """+selectq1+"""
    #                     FROM """+hana_db+""".pref_scores_mthly a
    #                     JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
    #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
    #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
    #                     join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
    #                     WHERE a.CATG_CD = %s
    #                     AND a.SUBCAT_CD = %s
    #                     AND a.SECTOR_SCENARIO_CD = %s 
    #                     and a.CHNL_CD = 0
    #                     AND a.FMT_CD = 0
    #                     and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
	# 		            group by quarter(a.PERIOD_ENDING_DATE)
    #                     order by quarter(a.PERIOD_ENDING_DATE);
    #                     """

    select_query = """  SELECT 
                        CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
                        """+selectq1+"""
                        FROM """+hana_db+""".pref_scores_mthly a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        WHERE a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.CHNL_CD = 0
                        AND a.FMT_CD = 0
                        and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
			            group by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                        order by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE));
                        """

    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

 
def get_input_variables_simulation(selectq,selectq1,inp_date,input_values):
     # update query to get data based on forecast scenario and COVID_FLAG
    select_query = """  SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR,
                        """+selectq+"""
                        """+selectq1+"""
                        FROM """+hana_db+""".pref_scores_mthly a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        WHERE a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.CHNL_CD = 0
                        AND a.FMT_CD = 0
                        and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
			            order by a.PERIOD_ENDING_DATE;
                        """
        
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows

#def get_first_day_of_the_quarter(p_date: date_class):
#    return datetime(p_date.year, 3 * ((p_date.month - 1) // 3) + 1, 1)


def get_input_variables_recrodset_simulation(selectq1,inp_date,input_values):
     # update query to get data based on forecast scenario and COVID_FLAG
    select_query = """  SELECT a.PERIOD_ENDING_DATE as MONTH_YEAR,
                        """+selectq1+"""
                        FROM """+hana_db+""".res_recordset_save a
                        JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                        WHERE a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.CHNL_CD = 0
                        AND a.FMT_CD = 0
                        and a.PERIOD_ENDING_DATE >= '"""+inp_date+"""'
			            order by a.PERIOD_ENDING_DATE;
                        """
        
    #print(select_query)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows



def get_quarter(date):
    return int((date.month - 1) / 3 + 1)

def get_first_day_of_the_quarter(date):
    quarter = get_quarter(date)
    if quarter == 2:
        quarter= 3
    #print("quarter :",quarter)
    return datetime.datetime(date.year, 3 * quarter - 2, 1).date()

def get_pre_year(date):
     return date - datetime.timedelta(days=545)

#date1 = datetime.date(2021,7,1)
#print(get_first_day_of_the_quarter(get_pre_year(date1)))


def ul_db_get_predict_date(input_values):
     
    select_query = """ select
                    min(a.PERIOD_ENDING_DATE)
                    FROM """+hana_db+""".res_recordset_save a
                    JOIN """+hana_db+""".category_type c ON c.CATG_CD = a.CATG_CD and DIV_CD =%s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = c.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name = 'Country'
                    join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                    WHERE 
                    a.CATG_CD = %s
                    AND JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                    AND a.SUBCAT_CD = %s
                    AND a.SECTOR_SCENARIO_CD = 1 
                    AND a.CHNL_CD = 0
                    and a.FMT_CD = 0 """
        
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows1 = cursor_data.fetchmany(1)
    pred_date = rows1[0][0] 
    #print(str(pred_date))
    #exit
    cursor_data.close()   
    # calculate current quarter
    quar_date = get_first_day_of_the_quarter(pred_date)    
    quar_str =quar_date.strftime('%Y-%m-%d') 
    
    # get previous year
    prev_date = get_first_day_of_the_quarter(get_pre_year(pred_date))
    prev_str = prev_date.strftime('%Y-%m-%d')
    return quar_str,prev_str
    
#input_values = (2,142,142,9,3)
#print(ul_db_get_predict_date(input_values))

def ul_generic_sim_growth_rate(tempDF,metric):
    
    #tempDF[:,metric] = pd.to_numeric(tempDF.loc[:,metric])
    #print(tempDF["QUARTER_YEAR"])
    #exit()
    tempDF[metric] = tempDF[metric].astype(float)
    #pivotDF = resultsDF.pivot(index='YEAR_QTR',columns='CHANNEL',values='VOLUME').reset_index()
    tempDF[["YEAR","MONTH"]] = tempDF["QUARTER_YEAR"].str.split("-",expand=True)
    tempDF["YEAR"] = tempDF["YEAR"].astype(int)
    # print(resultsDF)
    masterDF = pd.DataFrame()
    starting_year = min(tempDF[tempDF['MONTH']=='Q1']['YEAR'])+1
    year_list = list(tempDF[tempDF['MONTH']=='Q1']['YEAR'].unique())
    for year in year_list:
        if year >= starting_year:
            yearDF = tempDF[tempDF["YEAR"]== year][['MONTH',metric]]
            compyearDF = tempDF[tempDF["YEAR"]== year-1][['MONTH',metric]]
            resultDict = ul_growth_rate.grow_rate_calc(yearDF,compyearDF,"PERIOD_TYPE",metric,"PCT_GROWTH",metric,1)
            resultDF1 = pd.DataFrame(resultDict)
            resultDF1['YEAR'] =year
            masterDF = masterDF.append(resultDF1)

    return masterDF




def ul_db_simulation_hist_pred_data(parameters,output=0):
    """
    get the following data
    1. actual and forecasted values from res_recordset_save
    2. input variables group by channel and quarter
    
    Augs: parameter contains following values 
        geo_id(int):country id
        division: dvision code 
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_secenario : forecast scenario code 
        chennel : chennel code 
        metric(str): it has to be Volume/Value
               
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
        forecast_type = parameters[0]['FORECAST_TYPE']
        channel = parameters[0]['CHANNEL']
        region = parameters[0]['REGION']
        format_cd = parameters[0]['format']
        metric = parameters[0]['METRIC']
    
    else:        
        geo_id = parameters['COUNTRY_NAME']
        division = parameters['DIVISION']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_scenario = parameters['FORECAST_SCENARIO']
        forecast_type = parameters['FORECAST_TYPE']
        channel = parameters['CHANNEL']
        region = parameters['REGION']
        format_cd = parameters['FORMAT']
        metric = parameters['METRIC']
        
    for i in range(len(config_json)):
        # print(config_json[i]['geo_id'])
        if str(config_json[i]['geo_id']) == str(geo_id):
            input_var_ui = config_json[i]["simulation"]["variables_ui"]
            input_var_ui_pref = config_json[i]["simulation"]["variables_ui_pref"]
            calc_metric = config_json[i]["simulation"]["metric"]
            calc_metric_pref = config_json[i]["simulation"]["metric_pref"]
            
    
    input_selectQ1 = ""
    input_selectQ = ""
    input_var_len = len(input_var_ui)
    
    input_pref_selectQ1 = ""
    input_pref_selectQ = ""
    input_var_pref_len = len(input_var_ui_pref)
    
    
    forecast_type = forecast_type.lower()
    metric = metric.lower()
    
    input_values2 = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    #input_values2 = (3,73,73,10,14)
    
    pred_date, comp_date = ul_db_get_predict_date(input_values2)  
            
       
    if forecast_type == 'total':    
        
        for j in range(0, input_var_len):
            if j == input_var_len -1:
                input_selectQ = input_selectQ + input_var_ui[j]
                input_selectQ1 = input_selectQ1 + calc_metric[j]+"(IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var_ui[j]+"') as decimal),0)) as "+input_var_ui[j]
            else:
                input_selectQ = input_selectQ + input_var_ui[j]+","
                input_selectQ1 = input_selectQ1 + calc_metric[j]+"(IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var_ui[j]+"') as decimal),0)) as "+input_var_ui[j]+","
        
        
        
        column_names2 = ["QUARTER_YEAR"] + input_var_ui
        column_names3 = ["QUARTER_YEAR"] + input_var_ui_pref
        column_names1 = ["MONTH_YEAR","VOLUME" ,"RECORD_TYPE"] 
        
        
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
        
        
        if metric == "value" or metric == "volume":
            
            column_names1 = ["MONTH_YEAR","VOLUME", "VALUE","RECORD_TYPE"]
            selectq1 ="SALES_VOLUME,SALES_VALUE"
            selectq2 = "PREDICTED_VOLUME,PREDICTED_VALUE"
            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
            selectq4 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME, 
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""

           
            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
            
        
#        elif metric == "value":
#            
#            selectq1 ="SALES_VALUE"
#            selectq2 = "PREDICTED_VALUE"
#            selectq3 = """(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal)*
#                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
#            selectq4 = """ (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal)* 
#                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""
#        
#            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
#                        
#            
#        elif metric == "volume":
#            
#            selectq1 ="SALES_VOLUME"
#            selectq2 = "PREDICTED_VOLUME"
#            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal) as SALES_VOLUME"""
#            selectq4 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME"""
#        
#            
#            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
#        
            
       
        resultDF2 = pd.DataFrame(get_input_variables_data(input_selectQ,input_selectQ1,pred_date,input_values1),columns=column_names2)
       
        resultDF3 = pd.DataFrame(get_input_variables_data(input_selectQ,input_selectQ1,comp_date,input_values1),columns=column_names2)
        #print(str(pred_date)) # 2021-07-01
        #print(str(comp_date)) # 2020-01-01
        #print(str(resultDF2))
        #exit();
       
        input_values3 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
        


        if input_var_pref_len > 0:           
            
            for k in range(0, input_var_pref_len):
                if k == input_var_pref_len -1:
                    input_pref_selectQ = input_pref_selectQ + input_var_ui_pref[k]
                    input_pref_selectQ1 = input_pref_selectQ1 + calc_metric_pref[k]+"(IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*]."+input_var_ui_pref[k]+"') as decimal),0)) as "+input_var_ui_pref[k]
                else:
                    input_pref_selectQ = input_pref_selectQ + input_var_ui_pref[k]+","
                    input_pref_selectQ1 = input_pref_selectQ1 + calc_metric_pref[k]+"(IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*]."+input_var_ui_pref[k]+"') as decimal),0)) as "+input_var_ui_pref[k]+","

            resultDF2_pref = pd.DataFrame(get_input_variables_data_perf(input_pref_selectQ,input_pref_selectQ1,pred_date,input_values3),columns=column_names3)
            
            resultDF3_pref = pd.DataFrame(get_input_variables_data_perf(input_pref_selectQ,input_pref_selectQ1,comp_date,input_values3),columns=column_names3)
            # Sales value/volume data processing
            resultDF2 = resultDF2.merge(resultDF2_pref,on='QUARTER_YEAR')
            resultDF3 = resultDF3.merge(resultDF3_pref,on='QUARTER_YEAR')
        
                
        
        # to get updated column names from dataframe
        col_names = list(resultDF2.columns)
   
        updatedDF = pd.DataFrame()
        for col_name in col_names[1:]:
            tempDF1 = resultDF3[["QUARTER_YEAR",col_name]].copy()
            outputDF = ul_generic_sim_growth_rate(tempDF1,col_name)
           
            outputDF["PCT_GROWTH"] = outputDF["PCT_GROWTH"].astype(str)+'%'
            if len(updatedDF) <=0:
                updatedDF['QUARTER'] = outputDF["PERIOD_TYPE"]
                updatedDF['YEAR'] = outputDF["YEAR"]
                updatedDF[col_name] = outputDF["PCT_GROWTH"]
            else:
                updatedDF[col_name] = outputDF["PCT_GROWTH"]
             
        #print(updatedDF)       
        updatedDF = updatedDF.loc[updatedDF['QUARTER'].isin(['QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4'}
        updatedDF['QUARTER'].replace(replaceMetricsDict,inplace=True)
    
        resultDF1["MONTH_YEAR"] = resultDF1["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        resultDF1["VOLUME"] = resultDF1["VOLUME"].astype(float).round(2)
        
        
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
    
#        if metric == "value":
#             resultDF1["VOLUME"] = resultDF1["VOLUME"]*exch_rate
       

        # convert value data type and conversion    
        resultDF1["VALUE"] = resultDF1["VALUE"].astype(float)
        resultDF1["VALUE"] = resultDF1["VALUE"]*exch_rate
        resultDF1["VALUE"] = resultDF1["VALUE"].astype(float).round(2)
        resultDF1["RECORD_TYPE"] = resultDF1["RECORD_TYPE"].str.replace('PREDICTED','FORECASTED')
        
     
        #input variable data processing
        resultDF2[["YEAR","QUARTER"]] = resultDF2["QUARTER_YEAR"].str.split("-",expand=True)
        resultDF2.drop(['QUARTER_YEAR'],axis =1,inplace=True)
        #print("hello2")
        #exit()
        #input variable data processing
        resultDF3[["YEAR","QUARTER"]] = resultDF3["QUARTER_YEAR"].str.split("-",expand=True)
        resultDF3.drop(['QUARTER_YEAR'],axis =1,inplace=True)
        
        
        
       
       
        for i in range(1, len(col_names)):
            resultDF2[col_names[i]] = resultDF2[col_names[i]].astype(float).round(2)
            
        col_names2 = list(resultDF3.columns)
        for j in range(0, len(col_names2)):
            if (col_names2[j] !="YEAR") and (col_names2[j] !="QUARTER"):
                resultDF3[col_names2[j]] = resultDF3[col_names2[j]].astype(float).round(2)
            
        
        resultDF2 = resultDF2[["QUARTER","YEAR" ] + col_names[1:]]
        resultDF3 = resultDF3[["QUARTER","YEAR" ] + col_names[1:]]
        
        responseDict1 = resultDF1.to_dict(orient='records')
        responseDict2 = resultDF2.to_dict(orient='records')
        
        #****************************
        # QA testing
        
        # to check output
        #resultDF2.to_csv("input_table_ind.csv")
        #resultDF3.to_csv("input_table_ind2.csv")
        
        #*****************************************
        
        # send out dataframe to simulator function
        if(output == 1):
            return resultDF3
        
        
        # filter only rows which are matching input values dataframe
        resultDF2.set_index(['QUARTER','YEAR'],inplace=True)
        updatedDF['YEAR'] = updatedDF['YEAR'].astype(str)
        updatedDF.set_index(['QUARTER','YEAR'],inplace=True)
        
        # calculate & apply mask
        updatedDF = updatedDF[updatedDF.index.isin(resultDF2.index)].reset_index()
        #print(resultDF2.index)
        #print(updatedDF.index)
        
        responseDict3 = updatedDF.to_dict(orient='records')
        
        responseData ={"request_header":parameters,
                       "SALES_VOLUME": responseDict1,
                       "INPUT_VALUES":responseDict2,
                       "INPUT_VALUES_GROWTH":responseDict3}
        
        json_object = json.dumps(responseData)
        print("1")
        return json_object
   
    else:
        responseData ={"request_header":parameters,
                       "SALES_VOLUME": "",
                       "INPUT_VALUES":"",
                       "INPUT_VALUES_GROWTH":""}
        print("2")
        json_object = json.dumps(responseData)
        return json_object

#input_vaiables= {"COUNTRY_NAME":"142","DIVISION":"1","CATAGORY_NAME":"8","SUB_CATEGORY_NAME":"1","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"5","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#input_vaiables= {"COUNTRY_NAME":"73","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME":"6","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#input_vaiables= {"COUNTRY_NAME":"181","DIVISION":"2","CATAGORY_NAME":"9","SUB_CATEGORY_NAME":"2","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#input_vaiables= {"COUNTRY_NAME":"179","DIVISION":"2","CATAGORY_NAME":"5","SUB_CATEGORY_NAME":"11","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#output1 = ul_db_simulation_hist_pred_data(input_vaiables)
#print(output1)


 
# update script line 273 
def generate_decompose_dict(qtr,col_list,rowtuple):
    
    dict_str = ""
    dict_str = '"PERIOD_ENDING_DATE":[],'
    for j in range(2,len(col_list)):
        if j == (len(col_list)-1):
            dict_str = dict_str +'"'+ col_list[j] +'"' + ":[]"
        else:
            dict_str = dict_str +'"'+ col_list[j] +'"' + ":[],"
    
    
    decomposeDict = {}
    for item in qtr:
        if "PERIOD_ENDING_DATE" in decomposeDict.keys():
            decomposeDict["PERIOD_ENDING_DATE"].append(str(getattr(rowtuple,"YEAR"))+item)
        else:
            decomposeDict["PERIOD_ENDING_DATE"] = []
            decomposeDict["PERIOD_ENDING_DATE"].append(str(getattr(rowtuple,"YEAR"))+item)
        
        for j in range(2,len(col_list)):
            if col_list[j] in decomposeDict.keys():
                decomposeDict[col_list[j]].append(getattr(rowtuple,col_list[j]))
            else:
                decomposeDict[col_list[j]] = []
                decomposeDict[col_list[j]].append(getattr(rowtuple,col_list[j]))
            
                
            
    
    #print(decomposeDict)
    return decomposeDict       


def generate_decompose_dict_inputtable(qtr,col_list,rowtuple):
    
    dict_str = ""
    dict_str = '"PERIOD_ENDING_DATE":[],'
    for j in range(2,len(col_list)):
        if j == (len(col_list)-1):
            dict_str = dict_str +'"'+ col_list[j] +'"' + ":[]"
        else:
            dict_str = dict_str +'"'+ col_list[j] +'"' + ":[],"
    
    
    decomposeDict = {}
    for item in qtr:
        if "PERIOD_ENDING_DATE" in decomposeDict.keys():
            decomposeDict["PERIOD_ENDING_DATE"].append(str(getattr(rowtuple,"YEAR"))+item)
        else:
            decomposeDict["PERIOD_ENDING_DATE"] = []
            decomposeDict["PERIOD_ENDING_DATE"].append(str(getattr(rowtuple,"YEAR"))+item)
        
        for j in range(2,len(col_list)):
            value = getattr(rowtuple,col_list[j])
            if value !=0:
                value =1
            if col_list[j] in decomposeDict.keys():
                decomposeDict[col_list[j]].append(value)
            else:
                decomposeDict[col_list[j]] = []
                decomposeDict[col_list[j]].append(value)
            
    
    #print(decomposeDict)
    return decomposeDict


# column construct for input variables for both record set save and pref score monthly
def generic_column_construct(input_var_sim,col_name):
  
    input_selectQ1 = ""
    input_selectQ = ""
    input_var_len = len(input_var_sim)
    for j in range(0, input_var_len):
        if j == input_var_len -1:
            input_selectQ = input_selectQ + input_var_sim[j]
            input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a."+col_name+", '$."+input_var_sim[j]+"') as decimal),0) as "+input_var_sim[j]
        else:
            input_selectQ = input_selectQ + input_var_sim[j]+","
            input_selectQ1 = input_selectQ1 +"IFNULL(cast(JSON_VALUE(a."+col_name+", '$."+input_var_sim[j]+"') as decimal),0) as "+input_var_sim[j]+","
   
    return input_selectQ1


    
def ul_db_simulation_pred_sales_data(parameters):
    
#    try:
#        ret_json = json.dumps(ast.literal_eval(parameters))
#        #res_file.close()
#    except:
#        print("Error reading input values")
#        sys.exit()
    
    # testing
    #data_dict = json.loads(parameters)
    
    # online
    
    data_dict = parameters

    
    parameters = data_dict["request_header"]
    
    if type(parameters) == list:
        # param_len = len(parameters)
        geo_id = parameters[0]['COUNTRY_NAME']
        division = parameters[0]['DIVISION']
        catg_cd = parameters[0]['CATAGORY_NAME']
        sub_catg_cd = parameters[0]['SUB_CATEGORY_NAME']
        forecast_scenario = parameters[0]['FORECAST_SCENARIO']
        forecast_type = parameters[0]['FORECAST_TYPE']
        #channel = parameters[0]['CHANNEL']
        channel = 0
        region = parameters[0]['REGION']
        format_cd = parameters[0]['FORMAT']
        metric = parameters[0]['METRIC'].lower()
    
    else:
        geo_id = parameters['COUNTRY_NAME']
        division = parameters['DIVISION']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_scenario = parameters['FORECAST_SCENARIO']
        forecast_type = parameters['FORECAST_TYPE']
        #channel = parameters['CHANNEL']
        channel = 0
        region = parameters['REGION']
        format_cd = parameters['FORMAT']
        metric = parameters['METRIC'].lower()
        
    
   
    #json_edited_data = str(tweaked_variable_json).replace("'",'"')
    price_flag = 1
    tdp_flag = 1
    input_df = pd.DataFrame.from_dict(data_dict["INPUT_VALUES"], orient="columns")
    input_df['YEAR'] = input_df['YEAR'].astype(int)
    input_df.set_index(['QUARTER','YEAR'],inplace=True)
    input_df = input_df.astype(float).round(2)
    #value1 = input_df['PRICE_PER_VOL'].sum()
    #value2 = input_df['TDP'].sum()
    #value3 = input_df['CONSUMER_PRICE_INDEX'].sum()
    value4 = input_df['RETAIL_AND_RECREATION_PCT_CHANGE'].sum()
    value5 = input_df['RESIDENTIAL_PCT_CHANGE'].sum()
    
    
    # price_flag update based on input variables change 
    if (value4 !=0) or (value5 !=0):
        price_flag = 0
    else:
        price_flag = 1
    
    if (value4 !=0) or (value5 !=0):
        tdp_flag = 0
    else:
        tdp_flag = 1
    
    
    table_values_df = ul_db_simulation_hist_pred_data(parameters,1)
    table_values_df['YEAR'] = table_values_df['YEAR'].astype(int)
    table_values_df.set_index(['QUARTER','YEAR'],inplace=True)
    table_values_df = table_values_df.astype(float).round(2)
    
    #return("One")
    #exit()
    # filter only last 8 quarters from table data
    updated_input_df = table_values_df[table_values_df.index.isin(input_df.index)].reset_index()
    # print(updated_input_df)


    input_df.reset_index(inplace=True)
    table_values_df.reset_index(inplace=True)
    #print(table_values_df.dtypes)
    #updatedDF = updatedDF[updatedDF.index.isin(resultDF2.index)].reset_index()
    rows = input_df.shape[0]
    col_names =list(table_values_df.columns)
    col_names = col_names[2:]
    
    
    for col_name in col_names:
        for i in range(0,rows):
            if input_df.at[i,col_name]!=0:
                q1 = input_df.at[i,"QUARTER"]
                y = int(input_df.at[i,"YEAR"])
                #print(q1,y)
                new_year = y -1
                #idx = table_values_df.index[table_values_df[(table_values_df['QUARTER'] == q1) & (table_values_df['YEAR'] == str(new_year))]].tolist()
                value = table_values_df.loc[(table_values_df['QUARTER'] == q1) & (table_values_df['YEAR'] == new_year),col_name].values[0]            
                # print(new_year,value)
                updated_input_df.at[i,col_name] = (value * input_df.at[i,col_name])/100 + value
                #table_values_df.index[table_values_df]
                #print(table_values_df.at[i,col_name])
            
    
    
#    table_df = pd.DataFrame.from_dict(data_dict["TABLE_VALUES"], orient="columns")
#    table_df.set_index(['QUARTER','YEAR'],inplace=True)
#    table_df = table_df.astype(float).round(2)
#    updated_df = (table_df * input_df)/100 + table_df
#    updated_df.reset_index(inplace=True)
    
    #updated_input_df.reset_index(inplace=True)
    updated_df_col_list = list(updated_input_df.columns)
    
    #print(updated_input_df)
    Q1 = ["-01-01","-02-01","-03-01"]
    Q2 = ["-04-01","-05-01","-06-01"]
    Q3 = ["-07-01","-08-01","-09-01"]
    Q4 = ["-10-01","-11-01","-12-01"]
    masterDF = pd.DataFrame()
    for row in updated_input_df.itertuples(index=False):
        qtr = getattr(row,'QUARTER')
        #print(row)
        if qtr =='Q1':
            returnDF1 =pd.DataFrame(generate_decompose_dict(Q1,updated_df_col_list,row))
            #print(returnDF1)
            masterDF= masterDF.append(returnDF1,ignore_index=True)
        if qtr =='Q2':
            returnDF2 =pd.DataFrame(generate_decompose_dict(Q2,updated_df_col_list,row))
            masterDF= masterDF.append(returnDF2,ignore_index=True)
        if qtr =='Q3':
            returnDF3 =pd.DataFrame(generate_decompose_dict(Q3,updated_df_col_list,row))
            masterDF = masterDF.append(returnDF3,ignore_index=True)
        if qtr =='Q4':
            returnDF4 =pd.DataFrame(generate_decompose_dict(Q4,updated_df_col_list,row))
            masterDF= masterDF.append(returnDF4,ignore_index=True)
            
        masterDF["PERIOD_ENDING_DATE"] = masterDF["PERIOD_ENDING_DATE"].astype(str)
    
    # input_df.reset_index(inplace=True)
    input_df_col_list = list(input_df.columns)
    
    input_masterDF = pd.DataFrame()
    for row in input_df.itertuples(index=False):
        qtr = getattr(row,'QUARTER')
        #print(row)
        if qtr =='Q1':
            returnDF1 =pd.DataFrame(generate_decompose_dict_inputtable(Q1,input_df_col_list,row))
            #print(returnDF1)
            input_masterDF= input_masterDF.append(returnDF1,ignore_index=True)
        if qtr =='Q2':
            returnDF2 =pd.DataFrame(generate_decompose_dict_inputtable(Q2,input_df_col_list,row))
            input_masterDF= input_masterDF.append(returnDF2,ignore_index=True)
        if qtr =='Q3':
            returnDF3 =pd.DataFrame(generate_decompose_dict_inputtable(Q3,input_df_col_list,row))
            input_masterDF = input_masterDF.append(returnDF3,ignore_index=True)
        if qtr =='Q4':
            returnDF4 =pd.DataFrame(generate_decompose_dict_inputtable(Q4,input_df_col_list,row))
            input_masterDF= input_masterDF.append(returnDF4,ignore_index=True)
    
    input_masterDF["PERIOD_ENDING_DATE"] = masterDF["PERIOD_ENDING_DATE"].astype(str)
    input_masterDF.set_index(['PERIOD_ENDING_DATE'],inplace=True)
    #print(input_masterDF)
    
    input_masterDF_rev = input_masterDF.replace({0:1, 1:0})
    masterDF.set_index(['PERIOD_ENDING_DATE'],inplace=True)

    
    input_values2 = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    pred_date, comp_date = ul_db_get_predict_date(input_values2)
        
    for i in range(len(config_json)):
        # print(config_json[i]['geo_id'])
        if str(config_json[i]['geo_id']) == str(geo_id):
            input_var_sim = config_json[i]["simulation"]["variables_simulation"]
            input_var_sim_rs = config_json[i]["simulation"]["variables_recordset_sim"]
            input_var_ui_rs = config_json[i]["simulation"]["variables_ui"]
            input_var_ui = config_json[i]["simulation"]["variables_ui_pref"]
    
    
    if len(input_var_ui_rs) > 0:
    
        input_selectQ1 = generic_column_construct(input_var_ui_rs,"RECORDSET_CONTENTS_JSON")
        column_names2 = ["PERIOD_ENDING_DATE"] + input_var_ui_rs
        
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
       
        inputDF_temp1 = pd.DataFrame(get_input_variables_recrodset_simulation(input_selectQ1,pred_date,input_values1),columns=column_names2)
        inputDF_temp1["PERIOD_ENDING_DATE"] = inputDF_temp1["PERIOD_ENDING_DATE"].astype(str)
           
        for i in range(1, len(column_names2)):
            inputDF_temp1[column_names2[i]] = inputDF_temp1[column_names2[i]].astype(float)
        
        inputDF_temp1.set_index(['PERIOD_ENDING_DATE'],inplace=True)
        masterDF2 = inputDF_temp1.copy()
    
    if len(input_var_ui) > 0:
        input_selectQ1 = generic_column_construct(input_var_ui,"CONTENTS_JSON")
            
        column_names1 = ["PERIOD_ENDING_DATE"] + input_var_ui
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
        selectQ = " " 
            
        inputDF_temp2 = pd.DataFrame(get_input_variables_simulation(selectQ,input_selectQ1,pred_date,input_values1),columns=column_names1)
        inputDF_temp2["PERIOD_ENDING_DATE"] = inputDF_temp2["PERIOD_ENDING_DATE"].astype(str)
            
        for i in range(1, len(column_names1)):
            inputDF_temp2[column_names1[i]] = inputDF_temp2[column_names1[i]].astype(float)
    
        inputDF_temp2.set_index(['PERIOD_ENDING_DATE'],inplace=True)
        masterDF2 = inputDF_temp1.merge(inputDF_temp2,on='PERIOD_ENDING_DATE')


    
    # to get only values which are getting changed in the UI
    output1 = input_masterDF*masterDF
    #to get values from DB which are getting changed in the UI
    output2 = input_masterDF_rev*masterDF2
    # add to those
    masterDF3 = output1 + output2
    
    masterDF3.reset_index(inplace=False)
   
    input_selectQ1 = ""
    input_selectQ = ""
    input_var_len = len(input_var_sim)
    
    
    if input_var_len > 0:
       
        for j in range(0, input_var_len):
            if j == input_var_len -1:
                input_selectQ = input_selectQ + input_var_sim[j]
                input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$."+input_var_sim[j]+"') as decimal),0) as "+input_var_sim[j]
            else:
                input_selectQ = input_selectQ + input_var_sim[j]+","
                input_selectQ1 = input_selectQ1 +"IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$."+input_var_sim[j]+"') as decimal),0) as "+input_var_sim[j]+","
       
       
        selectQ = "a.UL_GEO_ID, a.CATG_CD, a.CHNL_CD, a.FMT_CD, a.SUBCAT_CD,"        
        column_names1 = ["PERIOD_ENDING_DATE","UL_GEO_ID","CATG_CD","CHNL_CD","FMT_CD","SUBCAT_CD"] + input_var_sim
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
       
        input_df2 = pd.DataFrame(get_input_variables_simulation(selectQ,input_selectQ1,pred_date,input_values1),columns=column_names1)
        input_df2["PERIOD_ENDING_DATE"] = input_df2["PERIOD_ENDING_DATE"].astype(str)
        
        for i in range(1, len(column_names1)):
            input_df2[column_names1[i]] = input_df2[column_names1[i]].astype(float)
        
        masterDF3 = input_df2.merge(masterDF3,on='PERIOD_ENDING_DATE')

        
    
    input_var_rs_len = len(input_var_sim_rs)
    
    if input_var_rs_len > 0:
        
        if input_var_len == 0:
            input_selectQ1 = "a.UL_GEO_ID, a.CATG_CD, a.CHNL_CD, a.FMT_CD, a.SUBCAT_CD,"
            input_selectQ = "UL_GEO_ID, CATG_CD, CHNL_CD, FMT_CD, SUBCAT_CD,"
            column_names2 = ["PERIOD_ENDING_DATE","UL_GEO_ID", "CATG_CD", "CHNL_CD", "FMT_CD", "SUBCAT_CD"] + input_var_sim_rs
        else:
             input_selectQ1 = ""
             input_selectQ = ""
             column_names2 = ["PERIOD_ENDING_DATE"] + input_var_sim_rs
        
        for j in range(0, input_var_rs_len):
            if j == input_var_rs_len -1:
                input_selectQ = input_selectQ + input_var_sim_rs[j]
                input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$."+input_var_sim_rs[j]+"') as decimal),0) as "+input_var_sim_rs[j]
            else:
                input_selectQ = input_selectQ + input_var_sim_rs[j]+","
                input_selectQ1 = input_selectQ1 +"IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$."+input_var_sim_rs[j]+"') as decimal),0) as "+input_var_sim_rs[j]+","
    
    
        input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
       
        input_df3 = pd.DataFrame(get_input_variables_recrodset_simulation(input_selectQ1,pred_date,input_values1),columns=column_names2)
        input_df3["PERIOD_ENDING_DATE"] = input_df3["PERIOD_ENDING_DATE"].astype(str)
           
        for i in range(1, len(column_names2)):
            input_df3[column_names2[i]] = input_df3[column_names2[i]].astype(float)
        
        
        masterDF3 = input_df3.merge(masterDF3,on='PERIOD_ENDING_DATE')
        
    
        
    finalDF = masterDF3.copy()
    finalDF.fillna(0,inplace=True)
    
    # price assumptions for baseline scenario
#    input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
#    price_columns =["PERIOD_ENDING_DATE","PRICE_PER_VOL"]
#    input_selectQ1 = "cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal) as PRICE_PER_VOL"
#    price_df1 = pd.DataFrame(get_input_variables_recrodset_simulation(input_selectQ1,pred_date,input_values1),columns=price_columns)
#    
    
    #finalDF.to_csv("sample_input_values_sim.csv",index=False)
    
    
    if str(geo_id) =='142':

        catdf1= pd.read_excel("country_indonesia_config.xlsx",sheet_name="total")
        region_cd = int(input_df2.iloc[0,1])   
        catdim = catdf1.loc[(catdf1['CATG_CD'] == int(catg_cd)) & (catdf1['SUBCAT_CD'] == int(sub_catg_cd)) & (catdf1['CHNL_CD'] == int(channel))  & (catdf1['FMT_CD'] == int(format_cd)) & (catdf1['RGN_CD'] == region_cd)]['CAT_DIMENSION_ID'].values[0]
    #    print(catdim)
    #    print(price_flag)
        output_DF = simulation_function_Indonesia.simulation(finalDF,price_flag,tdp_flag,catdim)
        #print(updated_df)
        #print(output_DF)
        output_DF['RECORD_TYPE'] = 'FORECASTED'
    
    elif str(geo_id) =='73':
        output_DF = Simulation_Function_India_Total.Simulation_Function_India_Total(finalDF,price_flag,tdp_flag)
        #print(updated_df)
        #print(output_DF)
        output_DF['RECORD_TYPE'] = 'FORECASTED'

    elif str(geo_id) =='181':
        output_DF = Simulation_function_UK.Simulation_Function_UK_Total(finalDF,price_flag,tdp_flag)
        #print(updated_df)
        #print(output_DF)
        output_DF['RECORD_TYPE'] = 'FORECASTED'

    elif str(geo_id) =='179':
        
        output_DF = Simulation_function_SA.Simulation_Function_SA_Total(finalDF,price_flag,tdp_flag)
        #print(updated_df)
        #print(output_DF)
        output_DF['RECORD_TYPE'] = 'FORECASTED'
    
        
    else:
        responseDict2 = finalDF.to_dict(orient='records')
        responseData ={"request_header":parameters,
                       "SALES_VOLUME": [],
                       "TABLE_VALUES":responseDict2}
        
        json_object = json.dumps(responseData)
    
        return json_object


    # to get exchange rate from ccy_exch_rate
    exrate_query = """select exch_rate from """+hana_db+""".ccy_exch_rate a
                    join """+hana_db+""".currency_type b on b.ccy_cd = a.from_ccy_cd and ctry_geo_id = ?  
                    where to_ccy_cd = 0"""
                        
    input_data =(geo_id)
    cursor_data = connection.cursor()
    cursor_data.execute(exrate_query,input_data)
    rows1 = cursor_data.fetchmany(1)
    exch_rate = float(rows1[0][0]) 
    cursor_data.close()
        
   
#    if metric == "value":
#        output_DF['PREDICTED_VOLUME'] = output_DF['PREDICTED_VOLUME']* finalDF['PRICE_PER_VOL']
#        output_DF['PREDICTED_VOLUME'] = output_DF['PREDICTED_VOLUME']* exch_rate
#    
    output_DF['PREDICTED_VOLUME'] = output_DF['PREDICTED_VOLUME'].astype(float).round(2)
    #output_DF['PRICE_PER_VOL'] = output_DF['PRICE_PER_VOL'].astype(float).round(2)
    output_DF.insert(2,'PREDICTED_VALUE', output_DF['PREDICTED_VOLUME']* output_DF['PRICE_PER_VOL'])
    #output_DF.insert(3,'PRICE_PER_VOL', finalDF['PRICE_PER_VOL'].round(2))
    #output_DF.insert(4,'EXCH_RATE', exch_rate)
   
    output_DF['PREDICTED_VALUE'] = round((output_DF['PREDICTED_VALUE']* exch_rate),2)
    
    
    
    
    #********************************************************************
    #QA check to validate input and out data for modeling
    #data check before simulation
    #finalDF.to_csv("sample_input_values_sim.csv",index=False)
    #output_DF.to_csv("model_output.csv")
    
    #******************************************************************
    final_cols = finalDF.columns
    
    for i in range(1, len(final_cols)):
            finalDF[final_cols[i]] = finalDF[final_cols[i]].astype(float).round(2)
    
    
    output_DF["PERIOD_ENDING_DATE"] = output_DF["PERIOD_ENDING_DATE"].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
    output_DF["PERIOD_ENDING_DATE"] = output_DF["PERIOD_ENDING_DATE"].apply(lambda x: x.strftime('%m/%d/%Y'))
    finalDF1 = output_DF[['PERIOD_ENDING_DATE','PREDICTED_VOLUME','PREDICTED_VALUE','RECORD_TYPE']].copy()
    
    
    responseDict1 = finalDF1.to_dict(orient='records')
    responseDict2 = finalDF.to_dict(orient='records')
    
    
    #print(output_DF)   
    responseData ={"request_header":parameters,
                       "SALES_VOLUME": responseDict1,
                       "TABLE_VALUES":responseDict2}
        
    json_object = json.dumps(responseData)
    
    return json_object
    
#    modeling file name creation, get region name for channel
#    select_query ="""select ul_rgn_geo_id from """+hana_db+""".ul_geo_hier 
#                        where ctry_geo_id = ? and ul_region_name ='Country';"""
#
#    
#    
#    input_values2 =(geo_id)
#    cursor_data = connection.cursor()
#    cursor_data.execute(select_query,input_values2)
#    rows = cursor_data.fetchmany(1)
#    region_code = int(rows[0][0])
#    filename = "_"+(3-len(str(catg_cd)))*'0'+str(catg_cd)+"_"+(3-len(str(sub_catg_cd)))*'0'+str(sub_catg_cd) +"_"+(3-len(str(channel)))*'0'+str(channel)+"_000"+ "_"+(3-len(str(region_code)))*'0'+str(region_code)+'.dat'
#    #print(filename) 
#    

def dummy_gen_sample_data():
    
    my_dict2 = dict()
    #cat_content = {"COUNTRY_NAME":"73","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME":"6","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
    #cat_content = {"COUNTRY_NAME":"142","DIVISION":"1","CATAGORY_NAME":"8","SUB_CATEGORY_NAME":"1","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"5","CHANNEL":"0","REGION":"0","FORMAT":"0"}
    #cat_content= {"COUNTRY_NAME":"181","DIVISION":"2","CATAGORY_NAME":"9","SUB_CATEGORY_NAME":"2","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
    cat_content= {"COUNTRY_NAME":"179","DIVISION":"1","CATAGORY_NAME":"2","SUB_CATEGORY_NAME":"4","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}

    request_header = {"request_header":cat_content}
    my_dict2.update(request_header)
        
    df1= pd.read_excel("simulation_demo_SA.xlsx",sheet_name="INPUT_VALUES")
    sales_vol_dict = {"INPUT_VALUES":(json.loads(df1.to_json(orient='records')))}
    my_dict2.update(sales_vol_dict)
    
    df2= pd.read_excel("simulation_demo_SA.xlsx",sheet_name="TABLE_VALUES")
    input_dict = {"TABLE_VALUES":(json.loads(df2.to_json(orient='records')))}
    my_dict2.update(input_dict)
    
    return (json.dumps(my_dict2))




#try:
#     with open("Simulator_response.json") as f:
#         ret_json = json.dumps(ast.literal_eval(f.read()))
#    #res_file.close()
#except:
#    print("Error reading config file")
#    sys.exit()
    
#print(ret_json)
#
#ret_json = {'request_header': {'COUNTRY_NAME': 142, 'CATAGORY_NAME': 4, 'SUB_CATEGORY_NAME': 4, 'METRIC': 'Volume', 'DIVISION': 2, 'CHANNEL': 1, 'FORECAST_SCENARIO': 1, 'FORECAST_TYPE': 'TOTAL', 'REGION': 0, 'FORMAT': 0}, 'INPUT_VALUES': [{'QUARTER': 'Q3', 'YEAR': '2021', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q4', 'YEAR': '2021', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q1', 'YEAR': '2022', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q2', 'YEAR': '2022', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q3', 'YEAR': '2022', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q4', 'YEAR': '2022', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q1', 'YEAR': '2023', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}, {'QUARTER': 'Q2', 'YEAR': '2023', 'PRICE_PER_VOL': 1, 'TDP': 0, 'RETAIL_AND_RECREATION_PCT_CHANGE': 0, 'RESIDENTIAL_PCT_CHANGE': 0, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 0, 'GDP_NOMINAL_LCU': 0, 'UNEMP_RATE': 0, 'CONSUMER_PRICE_INDEX': 0}], 'TABLE_VALUES': [{'QUARTER': 'Q3', 'YEAR': '2021', 'PRICE_PER_VOL': 19559.52, 'TDP': 4825.26, 'RETAIL_AND_RECREATION_PCT_CHANGE': -19, 'RESIDENTIAL_PCT_CHANGE': 10.32, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 1913840, 'GDP_NOMINAL_LCU': 4109380, 'UNEMP_RATE': 6.78, 'CONSUMER_PRICE_INDEX': 107.61}, {'QUARTER': 'Q4', 'YEAR': '2021', 'PRICE_PER_VOL': 19579.91, 'TDP': 4979.57, 'RETAIL_AND_RECREATION_PCT_CHANGE': -9.12, 'RESIDENTIAL_PCT_CHANGE': 4.45, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 1968196.67, 'GDP_NOMINAL_LCU': 4266053.33, 'UNEMP_RATE': 6.65, 'CONSUMER_PRICE_INDEX': 108.69}, {'QUARTER': 'Q1', 'YEAR': '2022', 'PRICE_PER_VOL': 19574.75, 'TDP': 5018.14, 'RETAIL_AND_RECREATION_PCT_CHANGE': -2.11, 'RESIDENTIAL_PCT_CHANGE': 0.96, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2003133.33, 'GDP_NOMINAL_LCU': 4315560, 'UNEMP_RATE': 6.42, 'CONSUMER_PRICE_INDEX': 109.39}, {'QUARTER': 'Q2', 'YEAR': '2022', 'PRICE_PER_VOL': 19513.03, 'TDP': 4905.88, 'RETAIL_AND_RECREATION_PCT_CHANGE': -0.5, 'RESIDENTIAL_PCT_CHANGE': 0.23, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2056806.67, 'GDP_NOMINAL_LCU': 4470553.33, 'UNEMP_RATE': 6.15, 'CONSUMER_PRICE_INDEX': 109.82}, {'QUARTER': 'Q3', 'YEAR': '2022', 'PRICE_PER_VOL': 19507.61, 'TDP': 4960.09, 'RETAIL_AND_RECREATION_PCT_CHANGE': -0.23, 'RESIDENTIAL_PCT_CHANGE': 0.11, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2122553.33, 'GDP_NOMINAL_LCU': 4654740, 'UNEMP_RATE': 5.91, 'CONSUMER_PRICE_INDEX': 110.38}, {'QUARTER': 'Q4', 'YEAR': '2022', 'PRICE_PER_VOL': 19513.1, 'TDP': 4978.8, 'RETAIL_AND_RECREATION_PCT_CHANGE': -0.18, 'RESIDENTIAL_PCT_CHANGE': 0.09, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2144813.33, 'GDP_NOMINAL_LCU': 4787843.33, 'UNEMP_RATE': 5.64, 'CONSUMER_PRICE_INDEX': 111.05}, {'QUARTER': 'Q1', 'YEAR': '2023', 'PRICE_PER_VOL': 19512.44, 'TDP': 5012.8, 'RETAIL_AND_RECREATION_PCT_CHANGE': -0.18, 'RESIDENTIAL_PCT_CHANGE': 0.09, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2161406.67, 'GDP_NOMINAL_LCU': 4807820, 'UNEMP_RATE': 5.41, 'CONSUMER_PRICE_INDEX': 111.67}, {'QUARTER': 'Q2', 'YEAR': '2023', 'PRICE_PER_VOL': 19510.22, 'TDP': 4903.73, 'RETAIL_AND_RECREATION_PCT_CHANGE': -0.17, 'RESIDENTIAL_PCT_CHANGE': 0.09, 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU': 2204380, 'GDP_NOMINAL_LCU': 4955596.67, 'UNEMP_RATE': 5.22, 'CONSUMER_PRICE_INDEX': 112.21}]}
##
#ret_json = dummy_gen_sample_data()
################
#ret2 = ul_db_simulation_pred_sales_data(ret_json)
#print(ret2)
    

def ul_db_simulation_hist_data_compare_scenarios(parameters):
    """
    get the following data
    1. actual and forecasted values from res_recordset_save
    2. input variables group by channel and quarter
    
    Augs: parameter contains following values 
        geo_id(int):country id
        division: dvision code 
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_secenario : forecast scenario code 
        chennel : chennel code 
        metric(str): it has to be Volume/Value
               
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
        forecast_type = parameters[0]['FORECAST_TYPE']
        channel = parameters[0]['CHANNEL']
        region = parameters[0]['REGION']
        format_cd = parameters[0]['format']
        metric = parameters[0]['METRIC']
    
    else:
        geo_id = parameters['COUNTRY_NAME']
        division = parameters['DIVISION']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_scenario = parameters['FORECAST_SCENARIO']
        forecast_type = parameters['FORECAST_TYPE']
        channel = parameters['CHANNEL']
        region = parameters['REGION']
        format_cd = parameters['FORMAT']
        metric = parameters['METRIC']
        
    for i in range(len(config_json)):
        # print(config_json[i]['geo_id'])
        if str(config_json[i]['geo_id']) == str(geo_id):
            input_var_ui = config_json[i]["simulation"]["variables_ui"]
            input_var_ui_pref = config_json[i]["simulation"]["variables_ui_pref"]
            input_var_sim = config_json[i]["simulation"]["variables_simulation"]
            input_var_sim_rs = config_json[i]["simulation"]["variables_recordset_sim"]
            
    
    
    input_var_ui = input_var_ui + input_var_sim_rs
    input_var_ui_pref = input_var_ui_pref + input_var_sim
    
#    print("record set",input_var_ui)
#    print("perf monthly ",input_var_ui_pref)
    
    
    input_selectQ1 = ""
    input_selectQ = ""
    input_var_len = len(input_var_ui)
    
    input_pref_selectQ1 = ""
    input_pref_selectQ = ""
    input_var_pref_len = len(input_var_ui_pref)
    
    
    forecast_type = forecast_type.lower()
    metric = metric.lower()
    
    
   
    column_names1 = ["PERIOD_ENDING_DATE","VOLUME", "VALUE","RECORD_TYPE"]
    column_names2 = ["PERIOD_ENDING_DATE","UL_GEO_ID","CATG_CD","CHNL_CD","FMT_CD","SUBCAT_CD"] + input_var_ui
    column_names3 = ["PERIOD_ENDING_DATE"] + input_var_ui_pref
    input_values1 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
    input_values2 = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    pred_date, comp_date = ul_db_get_predict_date(input_values2)  
    #return "a4a"
    
    if forecast_type == 'total':  
        
        if metric == "value" or metric == "volume":
            
            
            selectq1 ="SALES_VOLUME,SALES_VALUE"
            selectq2 = "PREDICTED_VOLUME,PREDICTED_VALUE"
            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
            selectq4 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME, 
                        (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""
        
            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
           
        
        
#        if metric == "value":
#            
#            selectq1 ="SALES_VALUE"
#            selectq2 = "PREDICTED_VALUE"
#            selectq3 = """(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal)*
#                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
#            selectq4 = """ (cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal)* 
#                        cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as PREDICTED_VALUE"""
#        
#            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
#                        
#            
#        if metric == "volume":
#            
#            selectq1 ="SALES_VOLUME"
#            selectq2 = "PREDICTED_VOLUME"
#            selectq3 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal) as SALES_VOLUME"""
#            selectq4 = """cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME"""
#        
#            
#            resultDF1 = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,selectq4,input_values1),columns=column_names1)
#        
            
        input_selectQ1 = "a.UL_GEO_ID, a.CATG_CD, a.CHNL_CD, a.FMT_CD, a.SUBCAT_CD,"
        input_values3 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
        
        for j in range(0, input_var_len):
            if j == input_var_len -1:
                input_selectQ = input_selectQ + input_var_ui[j]
                input_selectQ1 = input_selectQ1 + "IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var_ui[j]+"') as decimal),0) as "+input_var_ui[j]
            else:
                input_selectQ = input_selectQ + input_var_ui[j]+","
                input_selectQ1 = input_selectQ1 +"IFNULL(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*]."+input_var_ui[j]+"') as decimal),0) as "+input_var_ui[j]+","
        
        #return "dd"
        resultDF2 = pd.DataFrame(get_input_variables_recrodset_simulation(input_selectQ1,pred_date,input_values3),columns=column_names2)
        
      
        input_values3 = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)
    
        if input_var_pref_len > 0:
            selectQ = ""
            for k in range(0, input_var_pref_len):
                if k == input_var_pref_len -1:
                    input_pref_selectQ = input_pref_selectQ + input_var_ui_pref[k]
                    input_pref_selectQ1 = input_pref_selectQ1 + "IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$."+input_var_ui_pref[k]+"') as decimal),0) as "+input_var_ui_pref[k]
                else:
                    input_pref_selectQ = input_pref_selectQ + input_var_ui_pref[k]+","
                    input_pref_selectQ1 = input_pref_selectQ1 + "IFNULL(cast(JSON_VALUE(a.CONTENTS_JSON, '$."+input_var_ui_pref[k]+"') as decimal),0) as "+input_var_ui_pref[k]+","
            
            resultDF2_pref = pd.DataFrame(get_input_variables_simulation(selectQ,input_pref_selectQ1,pred_date,input_values3),columns=column_names3)
            # Sales value/volume data processing
            resultDF2 = resultDF2.merge(resultDF2_pref,on='PERIOD_ENDING_DATE')
           
        
        # to check output data
        #print(resultDF2)
       
        
        
        # to get updated column names from dataframe
           
        resultDF1["PERIOD_ENDING_DATE"] = resultDF1["PERIOD_ENDING_DATE"].apply(lambda x: x.strftime('%m/%d/%Y'))
        resultDF2["PERIOD_ENDING_DATE"] = resultDF2["PERIOD_ENDING_DATE"].apply(lambda x: x.strftime('%m/%d/%Y'))
        resultDF1["VOLUME"] = resultDF1["VOLUME"].astype(float).round(2)
        
        
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
    
        #if metric == "value":
        resultDF1["VALUE"] = resultDF1["VALUE"].astype(float) 
        resultDF1["VALUE"] = resultDF1["VALUE"]*exch_rate
        resultDF1["VALUE"] = resultDF1["VALUE"].round(2)

        # convert value data type and conversion    
        resultDF1 =  resultDF1[resultDF1["RECORD_TYPE"]=='FORECAST']
        resultDF1["RECORD_TYPE"] = resultDF1["RECORD_TYPE"].str.replace('PREDICTED','FORECASTED')
    


        #input variable data processing
        
        col_names = list(resultDF2.columns)
        
        for i in range(1, len(col_names)):
            resultDF2[col_names[i]] = resultDF2[col_names[i]].astype(float).round(2)
        
        #resultDF2 = resultDF2[["QUARTER","YEAR" ] + col_names[1:]]
        resultDF2.fillna(0,inplace=True)
        responseDict1 = resultDF1.to_dict(orient='records')
        responseDict2 = resultDF2.to_dict(orient='records')
        
        
        
        responseData ={"request_header":parameters,
                       "SALES_VOLUME": responseDict1,
                       "INPUT_VALUES":responseDict2
                     }
        
        json_object = json.dumps(responseData)
        return json_object
   
    else:
        responseData ={"request_header":parameters,
                       "SALES_VOLUME": "",
                       "INPUT_VALUES":""}
        
        json_object = json.dumps(responseData)
        return json_object
    
    
#input_vaiables= {"COUNTRY_NAME":"142","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME":"10","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#input_vaiables= {"COUNTRY_NAME":"73","DIVISION":"1","CATAGORY_NAME":"1","SUB_CATEGORY_NAME":"6","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#input_vaiables= {"COUNTRY_NAME":"181","DIVISION":"2","CATAGORY_NAME":"9","SUB_CATEGORY_NAME":"2","METRIC":"Volume", "FORECAST_TYPE":"TOTAL", "FORECAST_SCENARIO":"1","CHANNEL":"0","REGION":"0","FORMAT":"0"}
#output1 = ul_db_simulation_hist_data_compare_scenarios(input_vaiables)
#print(output1)
