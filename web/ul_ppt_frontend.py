# -*- coding: utf-8 -*-
"""
Created on Thu Aug 26 11:35:10 2021

@author: Rameshkumar
"""

import pandas as pd
# from hdbcli import dbapi
# import numpy as np
#import json
# from flask import jsonify
from functools import reduce
import ul_growth_rate
import ul_input_contribution
from sap_hana_credentials import connection
import json
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
    
import numpy as np

np.seterr(divide='ignore', invalid='ignore')


removelist =[(2019,'Q1'),(2019,'Q2'),(2019,'Q3'),(2019,'Q4'),(2023,'Q1'),(2023,'Q2'),(2023,'YEAR'),(2023,'Q3')]

removelist1 =[(2023,'Q1'),(2023,'Q2'),(2023,'YEAR'),(2023,'Q3')]

removelist2 =[(2023,'Q1'),(2023,'Q2'),(2023,'Q3')]

def get_sales_data(selectQ1,selectQ2,selectQ3, input_values):
    
    select_query = """select MONTH_YEAR,SUM("""+selectQ1+""") from
                        (
                        SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
                        """+selectQ2+"""
                        FROM """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = 1 
                        and a.CHNL_CD = 0
                        and a.FMT_CD = 0
                        GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                        union
                        SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR, 
                        """+selectQ3+"""
                        FROM """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = %s
                        and a.CHNL_CD = 0
                        and a.FMT_CD = 0
                        GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                        ) sub
                        GROUP BY MONTH_YEAR
                        ORDER BY MONTH_YEAR
                        """
    
    
    #print(input_values)
    cursor_data = connection.cursor()
    #print(select_query)
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


def ul_generic_ppt_growth_rate(tempDF,metric):
    
    #tempDF[:,metric] = pd.to_numeric(tempDF.loc[:,metric])
    tempDF[metric] = tempDF[metric].astype(float)
    #pivotDF = resultsDF.pivot(index='YEAR_QTR',columns='CHANNEL',values='VOLUME').reset_index()
    tempDF[["YEAR","MONTH"]] = tempDF["YEAR_QTR"].str.split("-",expand=True)
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


def ul_generic_ppt_growth_rate_vol_val(tempDF,metric):
    
    #tempDF[:,metric] = pd.to_numeric(tempDF.loc[:,metric])
    tempDF[metric] = tempDF[metric].astype(float)
    #pivotDF = resultsDF.pivot(index='YEAR_QTR',columns='CHANNEL',values='VOLUME').reset_index()
    tempDF[["YEAR","MONTH"]] = tempDF["YEAR_QTR"].str.split("-",expand=True)
    tempDF["YEAR"] = tempDF["YEAR"].astype(int)
    # print(resultsDF)
    masterDF = pd.DataFrame()
    starting_year = min(tempDF[tempDF['MONTH']=='Q1']['YEAR'])+1
    if starting_year < 2020:
        starting_year = 2020
    
    year_list = list(tempDF[tempDF['MONTH']=='Q1']['YEAR'].unique())
    for year in year_list:
        if year >= starting_year:
            yearDF = tempDF[tempDF["YEAR"]== year][['MONTH',metric]]
            compyearDF = tempDF[tempDF["YEAR"]== year-1][['MONTH',metric]]
            resultDict = ul_growth_rate.grow_rate_calc(yearDF,compyearDF,"PERIOD_TYPE",metric,"PCT_GROWTH",metric,1)
            resultDF1 = pd.DataFrame(resultDict)
            resultDF1['YEAR'] =year
            resultDF1['COMP_YEAR'] =year-1
            masterDF = masterDF.append(resultDF1)

    #compare 2019 Vs 2022 data
    yearDF = tempDF[tempDF["YEAR"]== 2022][['MONTH',metric]]
    compyearDF = tempDF[tempDF["YEAR"]== 2019][['MONTH',metric]]
    resultDict = ul_growth_rate.grow_rate_calc(yearDF,compyearDF,"PERIOD_TYPE",metric,"PCT_GROWTH",metric,1)
    resultDF1 = pd.DataFrame(resultDict)
    resultDF1['YEAR'] =2022
    resultDF1['COMP_YEAR'] =2019
    
    masterDF = masterDF.append(resultDF1)
    
    
    return masterDF


def ul_db_ppt_vol_val_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,vol_metric=0):
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        devision(int): devision
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_scenario(int): forecast scenario code        
    Returns:
        json_object(json object): json data to online 
        return values in json object : "PERIOD_TYPE","YEAR","SALE_VOLUME_PCT","SALE_VALUE_PCT","ABS_VALUE"
    """
    
    #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    column_names1 = ["YEAR_QTR","SALES_VOLUME"]
    selectq1 = "SALES_VOLUME"
    selectq2 ="SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME"
    selectq3 = "SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME"
                    
    volDF = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,input_values),columns=column_names1)
    #print(volDF.shape)
    if volDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        #print("In fun")
        return masterDF
    
    
    #print(volDF.head())
    resultDF1 = ul_generic_ppt_growth_rate_vol_val(volDF,"SALES_VOLUME")
    resultDF1.rename(columns={"PCT_GROWTH":"PCT_VOL_GROWTH"},inplace=True)
    
    column_names2 = ["YEAR_QTR","SALES_VALUE"]
    selectq1 = "SALES_VALUE"
    selectq2 ="""SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
    
    selectq3 ="""SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE"""
    
    valDF = pd.DataFrame(get_sales_data(selectq1,selectq2,selectq3,input_values),columns=column_names2)
    #print(valDF)
    if valDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    resultDF2 = ul_generic_ppt_growth_rate_vol_val(valDF,"SALES_VALUE")
    resultDF2.rename(columns={"PCT_GROWTH":"PCT_VAL_GROWTH"},inplace=True)
    
    
    if int(vol_metric) ==0:
    
        masterDF = pd.merge(resultDF1,resultDF2,on=["YEAR","PERIOD_TYPE","COMP_YEAR"])
        masterDF = masterDF[["YEAR","PERIOD_TYPE","COMP_YEAR","PCT_VOL_GROWTH","PCT_VAL_GROWTH","SALES_VALUE"]]
        
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        masterDF = masterDF[masterDF['SALES_VALUE'] > 0]
        masterDF['SALES_VALUE'] = round(masterDF['SALES_VALUE']/1000000,2)
        
        # update exchange rate for india
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
            # converting into EUR
        masterDF["SALES_VALUE"] = round(masterDF["SALES_VALUE"] * exch_rate,2)
        
        if len(removelist1) > 1:
            for item in removelist1:
                masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
    
        
        masterDF['PCT_VOL_GROWTH'] = masterDF['PCT_VOL_GROWTH'].astype(str) + '%'
        masterDF['PCT_VAL_GROWTH'] = masterDF['PCT_VAL_GROWTH'].astype(str) + '%'
        
        masterDF.rename(columns={"PCT_VOL_GROWTH":"% Vol Growth",
                               "PCT_VAL_GROWTH":"% Val Growth",
                               "SALES_VALUE":"Abs. Val(Mn EUR)"
                               },inplace=True)
        
        return masterDF
   
    else:
        masterDF = pd.merge(resultDF1,resultDF2,on=["YEAR","PERIOD_TYPE","COMP_YEAR"])
        masterDF = masterDF[["YEAR","PERIOD_TYPE","COMP_YEAR","PCT_VOL_GROWTH","PCT_VAL_GROWTH","SALES_VOLUME","SALES_VALUE"]]
        
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        masterDF = masterDF[masterDF['SALES_VALUE'] > 0]
        masterDF['SALES_VALUE'] = round(masterDF['SALES_VALUE']/1000000,2)
        masterDF['SALES_VOLUME'] = round(masterDF['SALES_VOLUME']/1000000,2)
        # update exchange rate for india
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
            # converting into EUR
        masterDF["SALES_VALUE"] = round(masterDF["SALES_VALUE"] * exch_rate,2)
        
        if len(removelist1) > 1:
            for item in removelist1:
                masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
    
        
        masterDF['PCT_VOL_GROWTH'] = masterDF['PCT_VOL_GROWTH'].astype(str) + '%'
        masterDF['PCT_VAL_GROWTH'] = masterDF['PCT_VAL_GROWTH'].astype(str) + '%'
        
        masterDF.rename(columns={"PCT_VOL_GROWTH":"% Vol Growth",
                               "PCT_VAL_GROWTH":"% Val Growth",
                               "SALES_VOLUME":"Abs. Vol(Mn)",
                               "SALES_VALUE":"Abs. Val(Mn EUR)"
                               },inplace=True)
        
        return masterDF
    
    


def ul_db_ppt_channel_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    resultDF = ul_growth_rate.ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    
    if isinstance(resultDF, pd.DataFrame):
        resultDF = resultDF[resultDF['VOLUME'] > 0]
        #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
        masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME_GROWTH').reset_index()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        if len(removelist) > 1:
            for item in removelist:
                masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
        
        columns_list = masterDF.columns
    
        for i in range(2, len(columns_list)):
            masterDF[columns_list[i]] = masterDF[columns_list[i]].astype(str) + '%'
        
        return masterDF
    else:
        resultDF = pd.DataFrame()
        return resultDF
    

def ul_db_ppt_channel_pct_contrib_indonesia(tempDF):
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
    roll_up_channels = ['Indonesia MT Minimarket','Indonesia MT Hyper/Super','Indonesia MT Key Account','Indonesia MT Independent']
    column_list = list(PivotDF.columns)
    #print(column_list)
    for channel in roll_up_channels:
        if channel in column_list:
            column_list.remove(channel)
            
    DF1 = PivotDF[column_list].copy()
    #print(DF1)
    DF2 = PivotDF[roll_up_channels].copy()
    
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
    
        DF2['MT_temp'] = DF1['Indonesia Modern Trade']
        DF2['MT_final'] = outputDF1['Indonesia Modern Trade']
        
        DF2['Indonesia MT Minimarket'] = (DF2['Indonesia MT Minimarket']/ DF2['MT_temp'])*(DF2['MT_final']/100)
        DF2['Indonesia MT Hyper/Super'] = (DF2['Indonesia MT Hyper/Super']/ DF2['MT_temp'])*(DF2['MT_final']/100)
        DF2['Indonesia MT Key Account'] = (DF2['Indonesia MT Key Account']/ DF2['MT_temp'])*(DF2['MT_final']/100)
        DF2['Indonesia MT Independent'] = (DF2['Indonesia MT Independent']/ DF2['MT_temp'])*(DF2['MT_final']/100)
        DF2 = DF2[['Indonesia MT Minimarket','Indonesia MT Hyper/Super','Indonesia MT Key Account','Indonesia MT Independent']]
        
        
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF
     

def ul_db_ppt_channel_pct_contrib_india(tempDF):
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
    roll_up_channels = ['GT - Rural','GT - Urban']
    column_list = list(PivotDF.columns)
    #print(column_list)
    for channel in roll_up_channels:
        if channel in column_list:
            column_list.remove(channel)
            
    DF1 = PivotDF[column_list].copy()
    DF2 = PivotDF[roll_up_channels].copy()
    
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        #print("inside first IF")
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
    
        DF2['GT_temp'] = DF1['GT']
        DF2['GT_final'] = outputDF1['GT']
        
        DF2['GT - Rural'] = (DF2['GT - Rural']/ DF2['GT_temp'])*(DF2['GT_final']/100)
        DF2['GT - Urban'] = (DF2['GT - Urban']/ DF2['GT_temp'])*(DF2['GT_final']/100)
        DF2 = DF2[['GT - Rural','GT - Urban']]
        
        
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF
     

def ul_db_ppt_channel_pct_contrib_Philippines_one(tempDF):
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
    roll_up_channels = ['Total Grocery Store','Total Convenience Store']
    column_list = list(PivotDF.columns)
    #print(column_list)
    for channel in roll_up_channels:
        if channel in column_list:
            column_list.remove(channel)
            
    DF1 = PivotDF[column_list].copy()
    DF2 = PivotDF[roll_up_channels].copy()
    
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        #print("inside first IF")
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
    
        DF2['Total_Grocery_temp'] = DF1['Total Grocery/CV']
        DF2['Total_Grocery_final'] = outputDF1['Total Grocery/CV']
        
        DF2['Total Grocery Store'] = (DF2['Total Grocery Store']/ DF2['Total_Grocery_temp'])*(DF2['Total_Grocery_final']/100)
        DF2['Total Convenience Store'] = (DF2['Total Convenience Store']/ DF2['Total_Grocery_temp'])*(DF2['Total_Grocery_final']/100)
        DF2 = DF2[['Total Grocery Store','Total Convenience Store']]
        
        
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF
     
def ul_db_ppt_channel_pct_contrib_Philippines_two(tempDF):
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
    roll_up_channels = ['Total Grocery Store','Total Convenience Store','Total Drugstores','Total Department Stores']
    column_list = list(PivotDF.columns)
    #print(column_list)
    for channel in roll_up_channels:
        if channel in column_list:
            column_list.remove(channel)
            
    
    DF1 = PivotDF[column_list].copy()
    DF2 = PivotDF[roll_up_channels].copy()
    
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        #print("inside first IF")
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
    
        DF2['Total_Grocery_temp'] = DF1['Total Grocery/CV']
        DF2['Total_Grocery_final'] = outputDF1['Total Grocery/CV']
        
        DF2['Total_Drugstore_temp'] = DF1['Total Drugstore/Department Sto']
        DF2['Total_Drugstore_final'] = outputDF1['Total Drugstore/Department Sto']
        
        
        # set 1
        DF2['Total Grocery Store'] = (DF2['Total Grocery Store']/ DF2['Total_Grocery_temp'])*(DF2['Total_Grocery_final']/100)
        DF2['Total Convenience Store'] = (DF2['Total Convenience Store']/ DF2['Total_Grocery_temp'])*(DF2['Total_Grocery_final']/100)
        
        #set 2
        DF2['Total Drugstores'] = (DF2['Total Drugstores']/ DF2['Total_Drugstore_temp'])*(DF2['Total_Drugstore_final']/100)
        DF2['Total Department Stores'] = (DF2['Total Department Stores']/ DF2['Total_Drugstore_temp'])*(DF2['Total_Drugstore_final']/100)
        
        
        
        DF2 = DF2[['Total Grocery Store','Total Convenience Store','Total Drugstores','Total Department Stores']]
        
        
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF
     

def generic_ppt_channel_pct_contrib(tempDF,geo_id,catg_cd,sub_catg_cd):

    select_query3 = """select a.chnl_desc, b.chnl_desc  
                    from """+hana_db+""".channel_type a
                    join """+hana_db+""".channel_type b on b.rollup_chnl = a. chnl_cd and a.geo_id = b.geo_id
                    where a.geo_id = %s
                    and b.chnl_cd in 
                    (select distinct CHNL_CD  
                    	from """+hana_db+""".res_recordset_save a 
                    	join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    	where a.CATG_CD = %s
                    	AND a.SUBCAT_CD = %s
                    	AND a.SECTOR_SCENARIO_CD = 1) """
    
    input_values = (geo_id,geo_id,catg_cd,sub_catg_cd)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values)
    rows = cursor_data.fetchall()
    column_names2 = ["chnl_desc","rollup_chnl"]
    rollup_channel_DF = pd.DataFrame(rows,columns=column_names2)
    cursor_data.close()
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
    column_list = list(PivotDF.columns)
    
    roll_up_channels = rollup_channel_DF['rollup_chnl'].to_list()
    main_channels = rollup_channel_DF['chnl_desc'].to_list()
    #print(roll_up_channels)
    #print(main_channels)
    

    #print(column_list)
    temp_chnl = roll_up_channels
    for channel in temp_chnl:
        if channel in column_list:
            column_list.remove(channel)
        else:
            roll_up_channels.remove(channel)
    
   
    
    if len(roll_up_channels) >1:
        DF2 = PivotDF[roll_up_channels].copy()
        DF1 = PivotDF[column_list].copy()
    elif len(roll_up_channels) == 1:
        DF2 = pd.DataFrame()
        DF1 = PivotDF[column_list +roll_up_channels].copy()
    else:
        DF2 = pd.DataFrame()
        DF1 = PivotDF[column_list].copy()
    
    
    #print("length of DF1",DF1.shape)
    #print("length of DF2",DF2.shape)
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        #print("inside first IF")
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
        
        for j in range(0,len(roll_up_channels)):
            DF2['temp'] = DF1[main_channels[j]]
            DF2['final'] = outputDF1[main_channels[j]]
            DF2[roll_up_channels[j]] = (DF2[roll_up_channels[j]]/ DF2['temp'])*(DF2['final']/100)
     
        DF2 = DF2[roll_up_channels]
       
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF
     
        
        

def ul_db_ppt_channel_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    resultDF = ul_growth_rate.ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    #print("inside function")
    #print(resultDF)
    if isinstance(resultDF, pd.DataFrame):
        
        select_query3 = """select chnl_cd, chnl_desc, rollup_chnl  
                            from """+hana_db+""".channel_type where geo_id = %s and rollup_chnl != 0"""
        
        input_values = (geo_id)
        
        cursor_data = connection.cursor()
        cursor_data.execute(select_query3,input_values)
        rows = cursor_data.fetchall()
        column_names2 = ["chnl_cd","chnl_desc","rollup_chnl"]
        rollup_channel_check = pd.DataFrame(rows,columns=column_names2)
        cursor_data.close()
        
        if rollup_channel_check.shape[0]> 0:
            resultDF = generic_ppt_channel_pct_contrib(resultDF,geo_id,catg_cd,sub_catg_cd)
            
            if len(removelist) > 1:
                for item in removelist:
                    resultDF.drop(resultDF[(resultDF['YEAR']==item[0])&(resultDF['PERIOD_TYPE']==item[1])].index,inplace = True)
                        
            columns_list = resultDF.columns
                    
            for i in range(2, len(columns_list)):
                resultDF[columns_list[i]] = resultDF[columns_list[i]].astype(str) + '%'
                        
            return resultDF
            
        
        else:
       
            resultDF = resultDF[resultDF['VOLUME'] > 0]
            #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
            masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='CHANNEL',values='VOLUME')
           
            resultDF = masterDF.div(masterDF.sum(axis=1), axis=0)
            resultDF = (resultDF*100).round(2)
            resultDF = resultDF.reset_index()
            
            resultDF = resultDF.loc[resultDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
            replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
            resultDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
            
            if len(removelist) > 1:
                for item in removelist:
                    resultDF.drop(resultDF[(resultDF['YEAR']==item[0])&(resultDF['PERIOD_TYPE']==item[1])].index,inplace = True)
                    
            columns_list = resultDF.columns
                
            for i in range(2, len(columns_list)):
                resultDF[columns_list[i]] = resultDF[columns_list[i]].astype(str) + '%'
                    
            return resultDF
    else:
        masterDF = pd.DataFrame()
        return masterDF
   

def ul_db_ppt_format_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    resultDF = ul_growth_rate.ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    
    if isinstance(resultDF, pd.DataFrame):
            
        resultDF = resultDF[resultDF['VOLUME'] > 0]
        #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
        
        masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='FORMAT',values='VOLUME_GROWTH').reset_index()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        if len(removelist) > 1:
                for item in removelist:
                    masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
            
        columns_list = masterDF.columns
        
        for i in range(2, len(columns_list)):
            masterDF[columns_list[i]] = masterDF[columns_list[i]].astype(str) + '%'
    
    
        return masterDF
    else:
        resultDF = pd.DataFrame()
        return resultDF
    
        
    
def ul_db_ppt_format_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    resultDF = ul_growth_rate.ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    
    if isinstance(resultDF, pd.DataFrame):
        resultDF = resultDF[resultDF['VOLUME'] > 0]
        #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
        masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='FORMAT',values='VOLUME')
        
        resultDF = masterDF.div(masterDF.sum(axis=1), axis=0)
        resultDF = (resultDF*100).round(2)
        resultDF = resultDF.reset_index()
        
        resultDF = resultDF.loc[resultDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        resultDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        if len(removelist) > 1:
                for item in removelist:
                    resultDF.drop(resultDF[(resultDF['YEAR']==item[0])&(resultDF['PERIOD_TYPE']==item[1])].index,inplace = True)
            
        columns_list = resultDF.columns
        
        for i in range(2, len(columns_list)):
            resultDF[columns_list[i]] = resultDF[columns_list[i]].astype(str) + '%'
    
    
        
        return resultDF
    
    else:
        resultDF = pd.DataFrame()
        return resultDF
    


def ul_db_ppt_region_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    resultDF = ul_growth_rate.ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    
    if isinstance(resultDF, pd.DataFrame):
        
        resultDF = resultDF[resultDF['VOLUME'] > 0]
        #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
        masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME_GROWTH').reset_index()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        if len(removelist) > 1:
            for item in removelist:
                masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
        
        columns_list = masterDF.columns
        
        for i in range(2, len(columns_list)):
            masterDF[columns_list[i]] = masterDF[columns_list[i]].astype(str) + '%'

        
        return masterDF
    else:
        resultDF = pd.DataFrame()
        return resultDF
        

def generic_ppt_region_pct_contrib(tempDF,geo_id,catg_cd,sub_catg_cd):

    if str(geo_id)=='142' and str(sub_catg_cd) == '16':
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = %s
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = %s
                        	AND aa.SUBCAT_CD = %s
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        and b.ul_region_name like '%_IC'
                        order by b.ul_region_name """
    
    
    
    elif str(geo_id)=='142':
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = %s
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = %s
                        	AND aa.SUBCAT_CD = %s
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        and b.ul_region_name not like '%_IC'
                        order by b.ul_region_name """
    
    
    else:
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = %s
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = %s
                        	AND aa.SUBCAT_CD = %s
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        order by b.ul_region_name """
                    
    
    
    
    input_values = (geo_id,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values)
    rows = cursor_data.fetchall()
    column_names2 = ["regn_desc","rollup_regn"]
    rollup_channel_DF = pd.DataFrame(rows,columns=column_names2)
    cursor_data.close()
    
    tempDF = tempDF[tempDF['VOLUME'] > 0]
    PivotDF = pd.pivot_table(tempDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME')
    column_list = list(PivotDF.columns)
    
    roll_up_regns = rollup_channel_DF['rollup_regn'].to_list()
    main_regns = rollup_channel_DF['regn_desc'].to_list()


    #print(column_list)
    for regn in roll_up_regns:
        if regn in column_list:
            column_list.remove(regn)
    

    
    DF1 = PivotDF[column_list].copy()
    DF2 = PivotDF[roll_up_regns].copy()
    
    
    if DF1.shape[0] > 1 and DF2.shape[0] > 1:
        #print("inside first IF")
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        FinalDF1 = outputDF1.reset_index()
    
        for j in range(0,len(roll_up_regns)):
            DF2['temp'] = DF1[main_regns[j]]
            DF2['final'] = outputDF1[main_regns[j]]
            DF2[roll_up_regns[j]] = (DF2[roll_up_regns[j]]/ DF2['temp'])*(DF2['final']/100)
     
        DF2 = DF2[roll_up_regns]
       
        outputDF2 = (DF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = pd.merge(FinalDF1,outputDF2,on=['YEAR','PERIOD_TYPE'])
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        return masterDF
    
    elif DF1.shape[0] > 1:
       
        outputDF1 = DF1.div(DF1.sum(axis=1), axis=0)
        outputDF1 = (outputDF1*100).round(2)
        outputDF1 = outputDF1.reset_index()
        
        masterDF = outputDF1.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
    
    elif DF2.shape[0] > 1:
        outputDF2 = DF2.div(DF2.sum(axis=1), axis=0)
        outputDF2 = (outputDF2*100).round(2)
        outputDF2 = outputDF2.reset_index()
        
        masterDF = outputDF2.copy()
        masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
        replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
        masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
        
        return masterDF
        
    else:
         masterDF = pd.DataFrame()
         return masterDF



def ul_db_ppt_region_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    resultDF = ul_growth_rate.ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario,"Volume","DF")
    
    if isinstance(resultDF, pd.DataFrame): 
        #print(resultDF)
        select_query3 = """select ul_prmry_rgn_geo_id from """+hana_db+""".ul_region_map a
                            join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = %s 
                            where ul_prmry_rgn_geo_id !=0"""
        
        input_values = (geo_id)
        
        cursor_data = connection.cursor()
        cursor_data.execute(select_query3,input_values)
        rows = cursor_data.fetchall()
        column_names2 = ["geo_id"]
        rollup_regn_check = pd.DataFrame(rows,columns=column_names2)
        cursor_data.close()
        
        if rollup_regn_check.shape[0]> 0:
            resultDF = generic_ppt_region_pct_contrib(resultDF,geo_id,catg_cd,sub_catg_cd)
            
           
        else:
        
            resultDF = resultDF[resultDF['VOLUME'] > 0]
            #resultDF['VOLUME_GROWTH'] = resultDF['VOLUME_GROWTH'].astype(str) + '%'
            masterDF = pd.pivot_table(resultDF,index=['YEAR','PERIOD_TYPE'],columns='REGION',values='VOLUME')
            
            resultDF = masterDF.div(masterDF.sum(axis=1), axis=0)
            resultDF = (resultDF*100).round(2)
            resultDF = resultDF.reset_index()
            
            resultDF = resultDF.loc[resultDF['PERIOD_TYPE'].isin(['YEAR','QTR1','QTR2','QTR3','QTR4'])]    
            replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
            resultDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
            
        
        if len(removelist) > 1:
            for item in removelist:
                resultDF.drop(resultDF[(resultDF['YEAR']==item[0])&(resultDF['PERIOD_TYPE']==item[1])].index,inplace = True)
        
        columns_list = resultDF.columns
        
        for i in range(2, len(columns_list)):
            resultDF[columns_list[i]] = resultDF[columns_list[i]].astype(str) + '%'
        
        
        return resultDF
    else:
        resultDF = pd.DataFrame()
        return resultDF


def ul_db_ppt_market_mix(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        devision(int): devision
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_scenario(int): forecast scenario code        
    Returns:
        json_object(json object): json data to online 
        return values in json object : "PERIOD_TYPE","YEAR","SALE_VOLUME_PCT","SALE_VALUE_PCT","ABS_VALUE"
    """
    
    #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    if str(geo_id) == '142':
    
        column_names2 = ["YEAR_QTR","TDP","PRICE_PER_VOL"]
        # select_query = """select MONTH_YEAR,avg(TDP),avg(PRICE_PER_VOL)
        #                     from
        #                     (
        #                     SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.TDP') as decimal)) as TDP,
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
        #                     FROM """+hana_db+""".RES_RECORDSET_SAVE a
        #                     join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = ?
        #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = ? and b.catg_cd = d.catg_cd
        #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
        #                     WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE') = 'ACTUAL' 
        #                     AND a.CATG_CD = ?
        #                     AND a.SUBCAT_CD = ?
        #                     AND a.SECTOR_SCENARIO_CD = 1
        #                     and a.CHNL_CD = 0
        #                     and a.FMT_CD = 0
        #                     and year(a.period_ending_date) > 2018
        #                     GROUP BY quarter(a.PERIOD_ENDING_DATE)
        #                     union
        #                     SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.TDP') as decimal)) as TDP,
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
        #                     FROM """+hana_db+""".RES_RECORDSET_SAVE a
        #                     join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = ?
        #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = ? and b.catg_cd = d.catg_cd
        #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
        #                     WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE') = 'FORECAST' 
        #                     AND a.CATG_CD = ?
        #                     AND a.SUBCAT_CD = ?
        #                     AND a.SECTOR_SCENARIO_CD = ?
        #                     and a.CHNL_CD = 0
        #                     and a.FMT_CD = 0
        #                     and year(a.period_ending_date) > 2018
        #                     GROUP BY quarter(a.PERIOD_ENDING_DATE)
        #                     )
        #                     GROUP BY MONTH_YEAR
        #                     ORDER BY MONTH_YEAR""" 

        select_query = """select MONTH_YEAR,avg(TDP),avg(PRICE_PER_VOL)
                            from
                            (
                            SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].TDP') as decimal)) as TDP,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
                            FROM """+hana_db+""".res_recordset_save a
                            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                            WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                            AND a.CATG_CD = %s
                            AND a.SUBCAT_CD = %s
                            AND a.SECTOR_SCENARIO_CD = 1
                            and a.CHNL_CD = 0
                            and a.FMT_CD = 0
                            and year(a.period_ending_date) > 2018
                            GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                            union
                            SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].TDP') as decimal)) as TDP,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
                            FROM """+hana_db+""".res_recordset_save a
                            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                            WHERE JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                            AND a.CATG_CD = %s
                            AND a.SUBCAT_CD = %s
                            AND a.SECTOR_SCENARIO_CD = %s
                            and a.CHNL_CD = 0
                            and a.FMT_CD = 0
                            and year(a.period_ending_date) > 2018
                            GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                            ) sub
                            GROUP BY MONTH_YEAR
                            ORDER BY MONTH_YEAR"""
        
         #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
        
        #print(input_values)
        cursor_data = connection.cursor()
        cursor_data.execute(select_query,input_values)
        rows = cursor_data.fetchall()
        cursor_data.close()
    
    else:
        column_names2 = ["YEAR_QTR","TDP","PRICE_PER_VOL"]
        # select_query = """  SELECT quarter(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.TDP') as decimal)) as TDP,
        #                     avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
        #                     FROM """+hana_db+""".RES_RECORDSET_SAVE a
        #                     join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = ?
        #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = ? and b.catg_cd = d.catg_cd
        #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
        #                     WHERE a.CATG_CD = ?
        #                     AND a.SUBCAT_CD = ?
        #                     AND a.SECTOR_SCENARIO_CD = ?
        #                     and a.CHNL_CD = 0
        #                     and a.FMT_CD = 0
        #                     and year(a.period_ending_date) > 2018
        #                     GROUP BY quarter(a.PERIOD_ENDING_DATE)
        #                     ORDER BY quarter(a.PERIOD_ENDING_DATE)""" 


        select_query = """  SELECT CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].TDP') as decimal)) as TDP,
                            avg(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as PRICE_PER_VOL
                            FROM """+hana_db+""".res_recordset_save a
                            join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                            join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                            join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                            WHERE a.CATG_CD = %s
                            AND a.SUBCAT_CD = %s
                            AND a.SECTOR_SCENARIO_CD = %s
                            and a.CHNL_CD = 0
                            and a.FMT_CD = 0
                            and year(a.period_ending_date) > 2018
                            GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                            ORDER BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))"""
        
         #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
        input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
        
        #print(input_values)
        cursor_data = connection.cursor()
        cursor_data.execute(select_query,input_values)
        rows = cursor_data.fetchall()
        cursor_data.close()
        print(str(rows))
    
    
    resultDF = pd.DataFrame(rows,columns=column_names2)
    if resultDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    resultDF1 = resultDF[["YEAR_QTR","TDP"]].copy()
    TDP_DF = ul_generic_ppt_growth_rate(resultDF1,"TDP")
    TDP_DF["PCT_GROWTH"] = TDP_DF["PCT_GROWTH"].astype(str)+'%' 
    TDP_DF.rename(columns={"PCT_GROWTH":"PCT_TDP_GROWTH"},inplace=True)
    
    resultDF2 = resultDF[["YEAR_QTR","PRICE_PER_VOL"]].copy()
    PPV_DF = ul_generic_ppt_growth_rate(resultDF2,"PRICE_PER_VOL")
    PPV_DF["PCT_GROWTH"] = PPV_DF["PCT_GROWTH"].astype(str)+'%'
    PPV_DF.rename(columns={"PCT_GROWTH":"PCT_PPV_GROWTH"},inplace=True)
        
    # merge DF
    masterDF = pd.merge(TDP_DF,PPV_DF,on=["YEAR","PERIOD_TYPE"])
    # formatting 
    masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['QTR1','QTR2','QTR3','QTR4'])]    
    replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
    masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
    
    masterDF = masterDF[masterDF['PRICE_PER_VOL'] > 0] 
   
    if len(removelist2) > 1:
        for item in removelist2:
            masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
      
     
    
    masterDF['PERIOD'] = masterDF['PERIOD_TYPE'] +' '+ masterDF['YEAR'].astype(str)
    masterDF = masterDF[["PERIOD","PCT_TDP_GROWTH","PCT_PPV_GROWTH"]]
    masterDF.rename(columns={"PCT_TDP_GROWTH":"Distribution Change",
                           "PCT_PPV_GROWTH":"Price Change"},inplace=True)
        
    
    
      
    
    
    return masterDF



def ul_db_ppt_contrib_macroeconomic(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    
    """
    forcast volume at country level  group by year and segment (channel,region and format)
    
    Augs:
        geo_id(int):country id
        devision(int): devision
        catg_cd(int): category code
        sub_catg_cd(int):sub category code
        forecast_scenario(int): forecast scenario code        
    Returns:
        json_object(json object): json data to online 
        return values in json object : "PERIOD_TYPE","YEAR","SALE_VOLUME_PCT","SALE_VALUE_PCT","ABS_VALUE"
    """
    
    #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    
    column_names2 = ["YEAR_QTR","DISPOSABLE_INCOME","UNEMP_RATE","GDP_REAL_LCU","CPI"]
    # select_query = """select  quarter(a.period_ending_date) as MONTH_YEAR,
    #                 avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$.PERSONAL_DISPOSABLE_INCOME_REAL_LCU') as decimal)) as PERSONAL_DISPOSABLE_INCOME_REAL_LCU,
    #                 avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$.UNEMP_RATE') as decimal)) as UNEMP_RATE,
    #                 avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$.GDP_REAL_LCU') as decimal)) as GDP_REAL_LCU,
    #                 avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$.CONSUMER_PRICE_INDEX') as decimal)) as CONSUMER_PRICE_INDEX
    #                 from """+hana_db+""".pref_scores_mthly a
    #                 join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
    #                 join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
    #                 join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
    #                 where a.catg_cd = %s
    #                 and a.subcat_cd = %s
    #                 AND a.SECTOR_SCENARIO_CD = %s 
    #                 and a.chnl_cd = 0
    #                 and a.fmt_cd = 0
    #                 and year(a.period_ending_date) > 2018
    #                 GROUP BY quarter(a.PERIOD_ENDING_DATE)
    #                 order by quarter(a.PERIOD_ENDING_DATE)""" 

    select_query = """select  
                    CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                    avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].PERSONAL_DISPOSABLE_INCOME_REAL_LCU') as decimal)) as PERSONAL_DISPOSABLE_INCOME_REAL_LCU,
                    avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].UNEMP_RATE') as decimal)) as UNEMP_RATE,
                    avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].GDP_REAL_LCU') as decimal)) as GDP_REAL_LCU,
                    avg(cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].CONSUMER_PRICE_INDEX') as decimal)) as CONSUMER_PRICE_INDEX
                    from """+hana_db+""".pref_scores_mthly a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    AND a.SECTOR_SCENARIO_CD = %s 
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    and year(a.period_ending_date) > 2018
                    GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                    order by CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))""" 
    
     #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
                    
    resultDF = pd.DataFrame(rows,columns=column_names2)
    
    if resultDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    resultDF1 = resultDF[["YEAR_QTR","DISPOSABLE_INCOME"]].copy()
    income_DF = ul_generic_ppt_growth_rate(resultDF1,"DISPOSABLE_INCOME")
    income_DF["PCT_GROWTH"] = income_DF["PCT_GROWTH"].astype(str)+'%'
    income_DF.rename(columns={"PCT_GROWTH":"PCT_INCOME_GROWTH"},inplace=True)
    
    resultDF2 = resultDF[["YEAR_QTR","UNEMP_RATE"]].copy()
    unemp_DF = ul_generic_ppt_growth_rate(resultDF2,"UNEMP_RATE")
    unemp_DF["PCT_GROWTH"] = unemp_DF["PCT_GROWTH"].astype(str)+'%'
    unemp_DF.rename(columns={"PCT_GROWTH":"PCT_UNEMP_GROWTH"},inplace=True)
    
    resultDF3 = resultDF[["YEAR_QTR","GDP_REAL_LCU"]].copy()
    GDP_DF = ul_generic_ppt_growth_rate(resultDF3,"GDP_REAL_LCU")
    GDP_DF["PCT_GROWTH"] = GDP_DF["PCT_GROWTH"].astype(str)+'%'
    GDP_DF.rename(columns={"PCT_GROWTH":"PCT_GDP_GROWTH"},inplace=True)
    
    resultDF4 = resultDF[["YEAR_QTR","CPI"]].copy()
    CPI_DF = ul_generic_ppt_growth_rate(resultDF4,"CPI")
    CPI_DF["PCT_GROWTH"] = CPI_DF["PCT_GROWTH"].astype(str)+'%'
    CPI_DF.rename(columns={"PCT_GROWTH":"PCT_CPI_GROWTH"},inplace=True)
    
    dfs = [income_DF,unemp_DF,GDP_DF,CPI_DF]
    
    masterDF = reduce(lambda left,right: pd.merge(left,right,on=["YEAR","PERIOD_TYPE"]), dfs)

    # formatting 
    masterDF = masterDF.loc[masterDF['PERIOD_TYPE'].isin(['QTR1','QTR2','QTR3','QTR4'])]    
    replaceMetricsDict = {'QTR1':'Q1','QTR2':'Q2','QTR3':'Q3','QTR4':'Q4','HY1':'H1','HY2':'H2'}
    masterDF['PERIOD_TYPE'].replace(replaceMetricsDict,inplace=True)
    
    masterDF = masterDF[masterDF['DISPOSABLE_INCOME'] > 0] 
    
    if len(removelist2) > 1:
        for item in removelist2:
            masterDF.drop(masterDF[(masterDF['YEAR']==item[0])&(masterDF['PERIOD_TYPE']==item[1])].index,inplace = True)
      
    
    
    masterDF['PERIOD'] = masterDF['PERIOD_TYPE'] +' '+ masterDF['YEAR'].astype(str)
    masterDF = masterDF[["PERIOD","PCT_INCOME_GROWTH","PCT_UNEMP_GROWTH","PCT_GDP_GROWTH","PCT_CPI_GROWTH"]]
    
    masterDF.rename(columns={"PCT_INCOME_GROWTH":"Disposable Income",
                             "PCT_UNEMP_GROWTH":"Unemployment",
                             "PCT_GDP_GROWTH":"GDP",
                             "PCT_CPI_GROWTH":"CPI"},inplace=True)
    
    
    return masterDF

def ul_db_estimates_by_Divisions(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    
    column_names2 = ["DIVISION","YEAR_QTR","VOLUME"]
    # select_query = """SELECT c.div_desc,quarter(a.PERIOD_ENDING_DATE), 
    #                 sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE')
    #                 when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal) * 
    #                 cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)
    #                 else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal) * 
    #                 cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)
    #                 end) as VALUE
    #                 FROM """+hana_db+""".res_recordset_save a
    #                 join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd 
    #                 join """+hana_db+""".division_type c on d.div_cd = c.div_cd
    #                 join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
    #                 WHERE 
    #                 a.SECTOR_SCENARIO_CD = %s 
    #                 and a.CHNL_CD = 0
    #                 and a.FMT_CD = 0
    #                 GROUP BY quarter(a.PERIOD_ENDING_DATE),c.div_desc
    #                 ORDER BY quarter(a.PERIOD_ENDING_DATE),c.div_desc""" 


    select_query = """SELECT c.div_desc,                    
                    CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)), 
                    sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                    when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                    else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                    end) as VALUE
                    FROM """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd 
                    join """+hana_db+""".division_type c on d.div_cd = c.div_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    WHERE 
                    a.SECTOR_SCENARIO_CD = %s 
                    and a.CHNL_CD = 0
                    and a.FMT_CD = 0
                    GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),c.div_desc
                    ORDER BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),c.div_desc""" 
  
     #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (geo_id,forecast_scenario)  
    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    segment = "DIVISION"                
    resultDF1 = pd.DataFrame(rows,columns=column_names2)
    

    select_query = """SELECT 'Total',
                CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)), 
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                end) as VALUE
                FROM """+hana_db+""".res_recordset_save a
                join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd 
                join """+hana_db+""".division_type c on d.div_cd = c.div_cd
                join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                WHERE 
                a.SECTOR_SCENARIO_CD = %s 
                and a.CHNL_CD = 0
                and a.FMT_CD = 0
                GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                ORDER BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))""" 
    
     #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (geo_id,forecast_scenario)  
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    #return str(rows)
    cursor_data.close()
    resultDF = pd.DataFrame(rows,columns=column_names2)
    resultDF = resultDF.append(resultDF1)
    
    if resultDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    segment = "DIVISION"
    
    output4 = ul_growth_rate.ul_generic_growth_rate(resultDF,segment)
    output4.rename(columns={"VOLUME":"VALUE","VOLUME_GROWTH":"VALUE_GROWTH"},inplace=True)
    output4["VALUE_GROWTH"] = output4["VALUE_GROWTH"].astype(str) + '%'
    prev_year = 2020
    current_year = 2021
    forecast_year = 2022
    
    
    # change currency conversion code for India
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
        # converting into EUR
    output4["VALUE"] = round(output4["VALUE"] * exch_rate,2)
        
    
    # previous year growth and pct
    output4['VALUE'] = round(output4['VALUE']/1000000,2)
    
    resultDF1 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==prev_year)]
    resultDF1 = resultDF1[[segment,'VALUE_GROWTH','VALUE']]
    resultDF1.rename(columns={"VALUE":"FY_"+str(prev_year) +"_Mkt_Value(Mn EUR)","VALUE_GROWTH":"FY_"+str(prev_year)+"_Mkt_Value_Growth(%)"},inplace=True)
     
    #resultDF1.columns = []
    # current year growth and pct
    resultDF2 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==current_year)]
    resultDF2 = resultDF2[[segment,'VALUE_GROWTH','VALUE']]
    resultDF2.rename(columns={"VALUE":"FY_"+str(current_year) +"_Mkt_Value(Mn EUR)","VALUE_GROWTH":"FY_"+str(current_year)+"_Mkt_Value_Growth(%)"},inplace=True)
    
    
    #current year H1 growth 
    resultDF3 = output4[(output4['PERIOD_TYPE']=='HY1') & (output4['YEAR']==current_year)]
    resultDF3 = resultDF3[[segment,'VALUE_GROWTH']]
    resultDF3.rename(columns={"VALUE_GROWTH":"H1_"+str(current_year)+"_Mkt Value_Growth(%)"},inplace=True)
    
    
    #current year H2 growth
    resultDF4 = output4[(output4['PERIOD_TYPE']=='HY2') & (output4['YEAR']==current_year)]
    resultDF4 = resultDF4[[segment,'VALUE_GROWTH']]
    resultDF4.rename(columns={"VALUE_GROWTH":"H2_"+str(current_year)+"_Mkt_Value_Fcst(%)"},inplace=True)
    
    
    # forecast year growth
    resultDF5 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==forecast_year)]
    resultDF5 = resultDF5[[segment,'VALUE_GROWTH']]
    resultDF5.rename(columns={"VALUE_GROWTH":"FY_"+str(forecast_year)+"_Mkt_Value_Fcst(%)"},inplace=True)
    
    dfs = [resultDF1,resultDF2,resultDF3,resultDF4,resultDF5]
    
    masterDF = reduce(lambda left,right: pd.merge(left,right,on=segment), dfs)
   
    return masterDF
    

def ul_db_estimates_by_categories(geo_id,division,catg_cd,sub_catg_cd,forecast_scenario):
    #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_scenario)  
    
    
    column_names2 = ["CATEGORY","YEAR_QTR","VOLUME"]
    
    # select_query = """SELECT d.catg_desc,quarter(a.PERIOD_ENDING_DATE), 
    #             sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE')
    #             when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal) * 
    #             cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)
    #             else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal) * 
    #             cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)
    #             end) as VALUE
    #             FROM """+hana_db+""".res_recordset_save a
    #             join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd 
    #             join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
    #             WHERE 
    #             a.SECTOR_SCENARIO_CD = %s 
    #             and a.CHNL_CD = 0
    #             and a.FMT_CD = 0
    #             GROUP BY quarter(a.PERIOD_ENDING_DATE),d.catg_desc
    #             ORDER BY quarter(a.PERIOD_ENDING_DATE),d.catg_desc""" 

    select_query = """SELECT d.catg_desc,CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)), 
                sum(case JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                when 'ACTUAL' then cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                else cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) * 
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)
                end) as VALUE
                FROM ULGROWTH20.res_recordset_save a
                join ULGROWTH20.category_type d on a.catg_cd = d.catg_cd 
                join ULGROWTH20.ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                WHERE 
                a.SECTOR_SCENARIO_CD = %s 
                and a.CHNL_CD = 0
                and a.FMT_CD = 0
                GROUP BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),d.catg_desc
                ORDER BY CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)),d.catg_desc""" 
    
     #request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"FORECASRT_SCENRIO":forecast_scenario}
    input_values = (geo_id,forecast_scenario)  
    #print(input_values)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    segment = "CATEGORY"                
    resultDF = pd.DataFrame(rows,columns=column_names2)
    #print(resultDF)
    #exit()
    
    if resultDF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    
    output4 = ul_growth_rate.ul_generic_growth_rate(resultDF,segment)


    output4.rename(columns={"VOLUME":"VALUE","VOLUME_GROWTH":"VALUE_GROWTH"},inplace=True)
    output4["VALUE_GROWTH"] = output4["VALUE_GROWTH"].astype(str) + '%'
    
    prev_year = 2020
    current_year = 2021
    forecast_year = 2022
    # change currency conversion code for India
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
        # converting into EUR
    output4["VALUE"] = round(output4["VALUE"] * exch_rate,2)
    
    # previous year growth and pct
    output4['VALUE'] = round(output4['VALUE']/1000000,2)
    
    resultDF1 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==prev_year)]
    resultDF1 = resultDF1[[segment,'VALUE_GROWTH','VALUE']]
    resultDF1.rename(columns={"VALUE":"FY_"+str(prev_year) +"_Mkt_Value(Mn EUR)","VALUE_GROWTH":"FY_"+str(prev_year)+"_Mkt_Value_Growth(%)"},inplace=True)
     
    #resultDF1.columns = []
    # current year growth and pct
    resultDF2 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==current_year)]
    resultDF2 = resultDF2[[segment,'VALUE_GROWTH','VALUE']]
    resultDF2.rename(columns={"VALUE":"FY_"+str(current_year) +"_Mkt_Value(Mn EUR)","VALUE_GROWTH":"FY_"+str(current_year)+"_Mkt_Value_Growth(%)"},inplace=True)
    
    
    #current year H1 growth 
    resultDF3 = output4[(output4['PERIOD_TYPE']=='HY1') & (output4['YEAR']==current_year)]
    resultDF3 = resultDF3[[segment,'VALUE_GROWTH']]
    resultDF3.rename(columns={"VALUE_GROWTH":"H1_"+str(current_year)+"_Mkt_Value_Growth(%)"},inplace=True)
    
    
    #current year H2 growth
    resultDF4 = output4[(output4['PERIOD_TYPE']=='HY2') & (output4['YEAR']==current_year)]
    resultDF4 = resultDF4[[segment,'VALUE_GROWTH']]
    resultDF4.rename(columns={"VALUE_GROWTH":"H2_"+str(current_year)+"_Mkt_Value_Fcst(%)"},inplace=True)
    
    
    # forecast year growth
    resultDF5 = output4[(output4['PERIOD_TYPE']=='YEAR') & (output4['YEAR']==forecast_year)]
    resultDF5 = resultDF5[[segment,'VALUE_GROWTH']]
    resultDF5.rename(columns={"VALUE_GROWTH":"FY_"+str(forecast_year)+"_Mkt_Value_Fcst(%)"},inplace=True)
    
    dfs = [resultDF1,resultDF2,resultDF3,resultDF4,resultDF5]
    
    masterDF = reduce(lambda left,right: pd.merge(left,right,on=segment), dfs)   
    return masterDF
    
def ul_db_ppt_forecast_vol(geo_id,division,catg_cd,sub_catg_cd):
    
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

    select_query = """select  a.period_ending_date as MONTH_YEAR,
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal) as SALES_VOLUME,
                cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal) as PREDICTED_VOLUME,
                JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as RECORD_TYPE,
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
    #input_values = ("1","169","169","8","1")
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)

    # request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names = ["MONTH_YEAR","SALES_VOLUME","PREDICTED_VOLUME","RECORD_TYPE","FORECAST_TYPE"]
    results_df = pd.DataFrame(rows,columns=column_names)
    cursor_data.close()
    
    if results_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        #print("joj")
        return masterDF
    
    #results_df["MONTH_YEAR"] = results_df["MONTH_YEAR"].apply(lambda x: x.strftime('%b-%y'))
    results_df["SALES_VOLUME"] = results_df["SALES_VOLUME"].astype(float)
    results_df["PREDICTED_VOLUME"] = results_df["PREDICTED_VOLUME"].astype(float)
    results_df["FORECAST_TYPE"] = results_df["FORECAST_TYPE"].str.replace(' ','_')
    # results_df["DATA_TYPE"] = results_df["DATA_TYPE"].str.replace('FORECAST','FORECASTED')

    # SHIV CHANGE - REMOVE DUPLICATE
    results_df = results_df.drop_duplicates(subset=["MONTH_YEAR", "FORECAST_TYPE"]) 

    results_df.loc[results_df["RECORD_TYPE"]=="ACTUAL", "PREDICTED_VOLUME"] = 0
    #output6.set_index(['MONTH_YEAR'])
    df1 = results_df[results_df["FORECAST_TYPE"]=="Baseline_forecast"]
    df1.set_index(['MONTH_YEAR'],inplace=True)

    
    pivotDF = results_df.pivot(index='MONTH_YEAR',columns='FORECAST_TYPE',values='PREDICTED_VOLUME')
    pivotDF.insert(0,"Actual",df1['SALES_VOLUME'])
    pivotDF.reset_index(inplace = True)
    
    # extend actual data to one more period to fix the gap in the chart
    #pred_stat_date = min(pivotDF[pivotDF['Actual']==0]['MONTH_YEAR'])
    pivotDF.fillna(0,inplace=True)
    act_end_date = max(pivotDF[pivotDF['Actual']!=0]['MONTH_YEAR'])
    value1 = pivotDF.loc[pivotDF['MONTH_YEAR']==act_end_date]['Actual'].values[0]
    new_col_names = pivotDF.columns
    for j in range(2,len(new_col_names)):
        pivotDF.loc[pivotDF['MONTH_YEAR']==act_end_date,new_col_names[j]] = value1
    
    pivotDF["MONTH_YEAR"] = pivotDF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    pivotDF.fillna("",inplace=True)
    pivotDF.replace(to_replace=0,value="",inplace=True)
    return pivotDF
    



def ul_db_ppt_input_contribution(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
    
    
#    request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,
#                           "FORECAST_SCENARIO": forecast_secenario}
#    

    input_values = (geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    
    column_names = ["YEAR","PERIOD_TYPE","Overall Volume Growth","Underlying Growth","Consumber Behaviour Changes",
               "Macro Economic Variables","Marketing Mix","Weather"]
   
    
    
    selectY = "BASE_YEAR"
    conditionY ="""AND BASE_YEAR = 2022
    group by BASE_YEAR order by BASE_YEAR;"""
    Year = pd.DataFrame(ul_input_contribution.get_data(selectY,conditionY,input_values),columns=column_names[:1]+column_names[2:])
    if Year.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    Year["PERIOD_TYPE"] = "YEAR"
    
    selectH1 = "BASE_YEAR, 'H1'"
    conditionH1 ="""AND BASE_YEAR = 2020
    and QTR in ('Q1','Q2') group by BASE_YEAR order by BASE_YEAR;"""
    H1_2020 = pd.DataFrame(ul_input_contribution.get_data(selectH1,conditionH1,input_values),columns=column_names)
    
    selectH2 = "BASE_YEAR, 'H2'"
    conditionH2 ="""AND BASE_YEAR = 2020
    and QTR in ('Q3','Q4') group by BASE_YEAR order by BASE_YEAR;"""
    H2_2020 = pd.DataFrame(ul_input_contribution.get_data(selectH2,conditionH2,input_values),columns=column_names)
    
    selectH1 = "BASE_YEAR, 'H1'"
    conditionH1 ="""AND BASE_YEAR = 2021
    and QTR in ('Q1','Q2') group by BASE_YEAR order by BASE_YEAR;"""
    H1_2021 = pd.DataFrame(ul_input_contribution.get_data(selectH1,conditionH1,input_values),columns=column_names)
    
    selectH2 = "BASE_YEAR, 'H2'"
    conditionH2 ="""AND BASE_YEAR = 2021
    and QTR in ('Q3','Q4') group by BASE_YEAR order by BASE_YEAR;"""
    H2_2021 = pd.DataFrame(ul_input_contribution.get_data(selectH2,conditionH2,input_values),columns=column_names)
    
    
    masterDF = pd.concat([H1_2020,H2_2020,H1_2021,H2_2021,Year], ignore_index=True,sort=False)
    
    for i in range(2, len(column_names)):
        masterDF[column_names[i]] = masterDF[column_names[i]].astype(float).round(2)

    #masterDF.drop(['Overall Volume Growth'],axis=1,inplace=True)
    #masterDF.insert(2,'Overall Volume Growth', round(masterDF[column_names[3:]].sum(axis=1),2))   
    return masterDF

def ul_db_ppt_retail_covid(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
   
    #rows = cursor_data.fetchmany(5)

    # request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
  
    
    select_query1 = """select  a.period_ending_date as MONTH_YEAR,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].RETAIL_AND_RECREATION_PCT_CHANGE') as decimal) as RETAIL_AND_RECREATION_PCT_CHANGE,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].RESIDENTIAL_PCT_CHANGE') as decimal) as RESIDENTIAL_PCT_CHANGE,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_CASES') as decimal) as NEW_CASES,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_DEATHS') as decimal) as NEW_DEATHS
                    from """+hana_db+""".pref_scores_mthly a 
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    AND a.SECTOR_SCENARIO_CD = %s
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    and year(a.period_ending_date) > 2018
                    order by period_ending_date"""

    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    column_names1 = ["MONTH_YEAR","RETAIL_AND_RECREATION_PCT_CHANGE","RESIDENTIAL_PCT_CHANGE","NEW_CASES","NEW_DEATHS"]
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query1,input_values)
    rows = cursor_data.fetchall()
    print(select_query1)
    print(str(rows))

    Retail_covid_df = pd.DataFrame(rows,columns=column_names1)
    Retail_covid_df.fillna(0,inplace=True)
    if Retail_covid_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    Retail_covid_df["MONTH_YEAR"] = Retail_covid_df["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    for i in range(1, len(column_names1)):
        Retail_covid_df[column_names1[i]] = Retail_covid_df[column_names1[i]].astype(float).round(2)
   
    
    return Retail_covid_df
    
def ul_db_ppt_channel_preference(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
   
       
    select_query2 = """select  a.period_ending_date as MONTH_YEAR,
                        c.chnl_desc,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].PREF_VALUE') as decimal) as PREF_VALUE
                        from """+hana_db+""".pref_scores_mthly a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        join """+hana_db+""".channel_type c on a.chnl_cd = c.chnl_cd and c.geo_id =%s
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        AND a.SECTOR_SCENARIO_CD = %s 
                        and a.chnl_cd != 0
                        and a.fmt_cd = 0
                        and year(a.period_ending_date) > 2018
                        order by period_ending_date"""

    input_values2 = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,forecast_secenario)
    
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query2,input_values2)
    rows = cursor_data.fetchall()
    column_names2 = ["MONTH_YEAR","CHANNEL","PREF_VALUE"]
    channel_perf_DF = pd.DataFrame(rows,columns=column_names2)
    print(select_query2)
    if channel_perf_DF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    channel_perf_DF["PREF_VALUE"] = channel_perf_DF["PREF_VALUE"].astype(float).round(2)
    
    # SHIV CHANGE - REMOVE DUPLICATE
    channel_perf_DF = channel_perf_DF.drop_duplicates(subset=["CHANNEL", "MONTH_YEAR"])    
    #print(channel_perf_DF.head(n=100))

    pivotDF = channel_perf_DF.pivot(index='CHANNEL',columns='MONTH_YEAR',values='PREF_VALUE')
    updatedDF = pivotDF.T
    updatedDF.sort_index(inplace=True)
    updatedDF.reset_index(inplace=True)
    updatedDF["MONTH_YEAR"] = updatedDF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    #print(str(updatedDF))
    return updatedDF
    

def ul_db_ppt_format_preference(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
   

    select_query3 = """select  a.period_ending_date as MONTH_YEAR,
                    c.fmt_desc,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].PREF_VALUE') as decimal) as PREF_VALUE
                    from """+hana_db+""".pref_scores_mthly a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    join """+hana_db+""".format_type c on a.fmt_cd = c.fmt_cd and c.geo_id =%s and c.catg_cd = %s and c.subcat_cd = %s
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    AND a.SECTOR_SCENARIO_CD = %s 
                    and a.chnl_cd = 0
                    and a.fmt_cd != 0
                    and year(a.period_ending_date) > 2018
                    order by period_ending_date"""

    input_values3 = (division,geo_id,geo_id,geo_id,catg_cd,sub_catg_cd,catg_cd,sub_catg_cd,forecast_secenario)
    
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values3)
    rows = cursor_data.fetchall()
    print(select_query3)
    print(str(rows))

    column_names2 = ["MONTH_YEAR","FORMAT","PREF_VALUE"]
    format_perf_DF = pd.DataFrame(rows,columns=column_names2)
    if format_perf_DF.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    #format_perf_DF["MONTH_YEAR"] = format_perf_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    format_perf_DF["PREF_VALUE"] = format_perf_DF["PREF_VALUE"].astype(float).round(2)

    # SHIV CHANGE - REMOVE DUPLICATE
    format_perf_DF = format_perf_DF.drop_duplicates(subset=["FORMAT", "MONTH_YEAR"]) 

    pivotDF = format_perf_DF.pivot(index='FORMAT',columns='MONTH_YEAR',values='PREF_VALUE')
    updatedDF2 = pivotDF.T
    updatedDF2.sort_index(inplace=True)
    updatedDF2.reset_index(inplace=True)
    updatedDF2["MONTH_YEAR"] = updatedDF2["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
    
    return updatedDF2



def ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,output):
   
    #rows = cursor_data.fetchmany(5)

    # request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
  
    
    
    if str(geo_id) == "245":
        
        column_names = ["MONTH_YEAR","SECTOR_SCENARIO_DESC","PERSONAL_DISPOSABLE_INCOME_REAL_LCU","UNEMP_RATE","GDP_REAL_LCU","CONSUMER_PRICE_INDEX",
                     "NEW_CASES","NEW_DEATHS","TRAFFIC_WEIGHT","RESIDENTIAL_PCT_CHANGE"]
        
        select_query1 = """select  a.period_ending_date as MONTH_YEAR,
                    e.SECTOR_SCENARIO_DESC,                
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].PERSONAL_DISPOSABLE_INCOME_REAL_LCU') as decimal) as PERSONAL_DISPOSABLE_INCOME_REAL_LCU,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].UNEMP_RATE') as decimal) as UNEMP_RATE,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].GDP_REAL_LCU') as decimal) as GDP_REAL_LCU,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].CONSUMER_PRICE_INDEX') as decimal) as CONSUMER_PRICE_INDEX,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_CASES') as decimal) as NEW_CASES,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_DEATHS') as decimal) as NEW_DEATHS,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].TRAFFIC_WEIGHT') as decimal) as TRAFFIC_WEIGHT,
                    cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].RESIDENTIAL_PCT_CHANGE') as decimal) as RESIDENTIAL_PCT_CHANGE
                    from """+hana_db+""".pref_scores_mthly a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    join """+hana_db+""".sector_scenario_type e on a.SECTOR_SCENARIO_CD = e.SECTOR_SCENARIO_CD
                    where a.catg_cd = %s
                    and a.subcat_cd = %s
                    AND a.SECTOR_SCENARIO_CD in(1,4,5)
                    and a.chnl_cd = 0
                    and a.fmt_cd = 0
                    order by a.PERIOD_ENDING_DATE"""
                    
        
    
    else:
    
        column_names = ["MONTH_YEAR","SECTOR_SCENARIO_DESC","PERSONAL_DISPOSABLE_INCOME_REAL_LCU","UNEMP_RATE","GDP_REAL_LCU","CONSUMER_PRICE_INDEX",
                     "NEW_CASES","NEW_DEATHS","RETAIL_AND_RECREATION_PCT_CHANGE","RESIDENTIAL_PCT_CHANGE"]
    
        select_query1 = """select  a.period_ending_date as MONTH_YEAR,
                        e.SECTOR_SCENARIO_DESC,                
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].PERSONAL_DISPOSABLE_INCOME_REAL_LCU') as decimal) as PERSONAL_DISPOSABLE_INCOME_REAL_LCU,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].UNEMP_RATE') as decimal) as UNEMP_RATE,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].GDP_REAL_LCU') as decimal) as GDP_REAL_LCU,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].CONSUMER_PRICE_INDEX') as decimal) as CONSUMER_PRICE_INDEX,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_CASES') as decimal) as NEW_CASES,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].NEW_DEATHS') as decimal) as NEW_DEATHS,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].RETAIL_AND_RECREATION_PCT_CHANGE') as decimal) as RETAIL_AND_RECREATION_PCT_CHANGE,
                        cast(JSON_VALUE(a.CONTENTS_JSON, '$[*].RESIDENTIAL_PCT_CHANGE') as decimal) as RESIDENTIAL_PCT_CHANGE
                        from """+hana_db+""".pref_scores_mthly a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        join """+hana_db+""".sector_scenario_type e on a.SECTOR_SCENARIO_CD = e.SECTOR_SCENARIO_CD
                        where a.catg_cd = %s
                        and a.subcat_cd = %s
                        AND a.SECTOR_SCENARIO_CD in(1,4,5)
                        and a.chnl_cd = 0
                        and a.fmt_cd = 0
                        order by a.PERIOD_ENDING_DATE"""

    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query1,input_values)
    rows = cursor_data.fetchall()
    
    macro_covid_df = pd.DataFrame(rows,columns=column_names)
    if macro_covid_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        #print("aa")
        return masterDF
    
    #macro_covid_df["MONTH_YEAR"] = macro_covid_df["MONTH_YEAR"].apply(lambda x: x.strftime('%d/%m/%Y'))
    for i in range(2, len(column_names)):
        macro_covid_df[column_names[i]] = macro_covid_df[column_names[i]].astype(float).round(2)
   
    if output == 'CPI':
        tempDF1 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","CONSUMER_PRICE_INDEX"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF1 = tempDF1.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

        pivotDF = tempDF1.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='CONSUMER_PRICE_INDEX')
        CPI_DF = pivotDF.T
        CPI_DF.sort_index(inplace=True)
        CPI_DF.reset_index(inplace=True)
        CPI_DF["MONTH_YEAR"] = CPI_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        CPI_DF.fillna(0,inplace=True)
        return CPI_DF
    
    if output == 'GDP':
        tempDF2 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","GDP_REAL_LCU"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF2 = tempDF2.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

        pivotDF = tempDF2.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='GDP_REAL_LCU')
        GDP_DF = pivotDF.T
        GDP_DF.sort_index(inplace=True)
        GDP_DF.reset_index(inplace=True)
        GDP_DF["MONTH_YEAR"] = GDP_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        GDP_DF.fillna(0,inplace=True)
        return GDP_DF
        
    if output == 'covid_new':
        tempDF3 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","NEW_CASES"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF3 = tempDF3.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

        pivotDF = tempDF3.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='NEW_CASES')
        covid_new_DF = pivotDF.T
        covid_new_DF.sort_index(inplace=True)
        covid_new_DF.reset_index(inplace=True)
        covid_new_DF["MONTH_YEAR"] = covid_new_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        covid_new_DF.fillna(0,inplace=True)
        return covid_new_DF
        
    if output == 'covid_death':
        
        tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","NEW_DEATHS"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 


        pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='NEW_DEATHS')
        covid_death_DF = pivotDF.T
        covid_death_DF.sort_index(inplace=True)
        covid_death_DF.reset_index(inplace=True)
        covid_death_DF["MONTH_YEAR"] = covid_death_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        covid_death_DF.fillna(0,inplace=True)
        return covid_death_DF
    
    if output == 'PDI':
        
        tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","PERSONAL_DISPOSABLE_INCOME_REAL_LCU"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

        pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='PERSONAL_DISPOSABLE_INCOME_REAL_LCU')
        PDI_DF = pivotDF.T
        PDI_DF.sort_index(inplace=True)
        PDI_DF.reset_index(inplace=True)
        PDI_DF["MONTH_YEAR"] = PDI_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        PDI_DF.fillna(0,inplace=True)
        return PDI_DF
    
    if output == 'UNEMP_RATE':
        
        tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","UNEMP_RATE"]]

        # SHIV CHANGE - REMOVE DUPLICATE
        tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

        pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='UNEMP_RATE')
        UMEMP_DF = pivotDF.T
        UMEMP_DF.sort_index(inplace=True)
        UMEMP_DF.reset_index(inplace=True)
        UMEMP_DF["MONTH_YEAR"] = UMEMP_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
        UMEMP_DF.fillna(0,inplace=True)
        return UMEMP_DF
    
    if output == 'RARPC':
        
        if str(geo_id) == "245":
            
            tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","TRAFFIC_WEIGHT"]]

            # SHIV CHANGE - REMOVE DUPLICATE
            tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

            pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='TRAFFIC_WEIGHT')
            RARPC_DF = pivotDF.T
            RARPC_DF.sort_index(inplace=True)
            RARPC_DF.reset_index(inplace=True)
            RARPC_DF["MONTH_YEAR"] = RARPC_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
            RARPC_DF.fillna(0,inplace=True)
            
            return RARPC_DF
        
        
        else:
            tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","RETAIL_AND_RECREATION_PCT_CHANGE"]]

            # SHIV CHANGE - REMOVE DUPLICATE
            tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 

            pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='RETAIL_AND_RECREATION_PCT_CHANGE')
            RARPC_DF = pivotDF.T
            RARPC_DF.sort_index(inplace=True)
            RARPC_DF.reset_index(inplace=True)
            RARPC_DF["MONTH_YEAR"] = RARPC_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
            RARPC_DF.fillna(0,inplace=True)
            
            return RARPC_DF
        
    if output == 'RPC':
        
        if str(geo_id) == "245":
           RPC_DF = pd.DataFrame()
        
        else:
            tempDF4 = macro_covid_df[["MONTH_YEAR","SECTOR_SCENARIO_DESC","RESIDENTIAL_PCT_CHANGE"]]

            # SHIV CHANGE - REMOVE DUPLICATE
            tempDF4 = tempDF4.drop_duplicates(subset=["SECTOR_SCENARIO_DESC", "MONTH_YEAR"]) 
            
            pivotDF = tempDF4.pivot(index='SECTOR_SCENARIO_DESC',columns='MONTH_YEAR',values='RESIDENTIAL_PCT_CHANGE')
            RPC_DF = pivotDF.T
            RPC_DF.sort_index(inplace=True)
            RPC_DF.reset_index(inplace=True)
            RPC_DF["MONTH_YEAR"] = RPC_DF["MONTH_YEAR"].apply(lambda x: x.strftime('%m/%d/%Y'))
            RPC_DF.fillna(0,inplace=True)
            
        return RPC_DF
        
    
    
    

def ul_db_ppt_val_vol_pct_change_year(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario):
   
    # select_query = """select MONTH_YEAR,sum(SALES_VOLUME),sum(SALES_VALUE) from
    #                 (
    #                 SELECT year(a.PERIOD_ENDING_DATE) as MONTH_YEAR, 
    #                 SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal)) as SALES_VOLUME,
    #                 SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.SALES_VOLUME') as decimal)* 
    #                 cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as SALES_VALUE
    #                 FROM """+hana_db+""".RES_RECORDSET_SAVE a
    #                 join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = ?
    #                 join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = ? and b.catg_cd = d.catg_cd
    #                 join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
    #                 where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE') = 'ACTUAL'
    #                 AND a.CATG_CD = ?
    #                 AND a.SUBCAT_CD = ?
    #                 AND a.SECTOR_SCENARIO_CD = 1 
    #                 and a.CHNL_CD = 0
    #                 and a.FMT_CD = 0
    #                 and year(a.PERIOD_ENDING_DATE) > 2018
    #                 GROUP BY year(a.PERIOD_ENDING_DATE)
    #                 union
    #                 SELECT year(a.PERIOD_ENDING_DATE) as MONTH_YEAR,
    #                 SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal)) as SALES_VOLUME, 
    #                 SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PREDICTED_VOLUME') as decimal)* 
    #                 cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.PRICE_PER_VOL') as decimal)) as SALES_VALUE
    #                 FROM """+hana_db+""".RES_RECORDSET_SAVE a
    #                 join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = ?
    #                 join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = ? and b.catg_cd = d.catg_cd
    #                 join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
    #                 where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$.RECORD_TYPE') = 'FORECAST'
    #                 AND a.CATG_CD = ?
    #                 AND a.SUBCAT_CD = ?
    #                 AND a.SECTOR_SCENARIO_CD = ?
    #                 and a.CHNL_CD = 0
    #                 and a.FMT_CD = 0
    #                 and year(a.PERIOD_ENDING_DATE) <2023
    #                 GROUP BY year(a.PERIOD_ENDING_DATE)
    #                 )
    #                 GROUP BY MONTH_YEAR
    #                 ORDER BY MONTH_YEAR"""


    select_query = """select MONTH_YEAR,sum(SALES_VOLUME),sum(SALES_VALUE) from
                    (
                    SELECT year(a.PERIOD_ENDING_DATE) as MONTH_YEAR,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)) as SALES_VOLUME,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE
                    FROM """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                    AND a.CATG_CD = %s
                    AND a.SUBCAT_CD = %s
                    AND a.SECTOR_SCENARIO_CD = 1
                    and a.CHNL_CD = 0
                    and a.FMT_CD = 0
                    and year(a.PERIOD_ENDING_DATE) > 2018
                    GROUP BY year(a.PERIOD_ENDING_DATE)
                    union
                    SELECT year(a.PERIOD_ENDING_DATE) as MONTH_YEAR,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)) as SALES_VOLUME,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)*
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE
                    FROM """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                    where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST'
                    AND a.CATG_CD = %s
                    AND a.SUBCAT_CD = %s
                    AND a.SECTOR_SCENARIO_CD = %s
                    and a.CHNL_CD = 0
                    and a.FMT_CD = 0
                    and year(a.PERIOD_ENDING_DATE) <2023
                    GROUP BY year(a.PERIOD_ENDING_DATE)
                    ) sub
                    GROUP BY MONTH_YEAR
                    ORDER BY MONTH_YEAR"""


    
    #rows = cursor_data.fetchmany(5)

    # request_header = {"COUNTRY_NAME":geo_id,"DIVISION":division,"CATEGORY_NAME":catg_cd,"SUB_CATEGORY_NAME":sub_catg_cd,"METRIC":metric}
    column_names1 = ["MONTH_YEAR","BS_SALES_VOLUME","BS_SALES_VALUE"]
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,"1")
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    baseline_df = pd.DataFrame(rows,columns=column_names1)
    if baseline_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    

    column_names2 = ["MONTH_YEAR","CS_SALES_VOLUME","CS_SALES_VALUE"]    
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,"2")
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cb_df = pd.DataFrame(rows,columns=column_names2)
    if cb_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    column_names3 = ["MONTH_YEAR","LVE_SALES_VOLUME","LVE_SALES_VALUE"]  
    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd,division,geo_id,geo_id,catg_cd,sub_catg_cd,"4")
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    lve_df = pd.DataFrame(rows,columns=column_names3)
    if lve_df.shape[0] < 1:
        masterDF = pd.DataFrame()
        return masterDF
    
    cursor_data.close()
   
    dfs = [baseline_df,cb_df,lve_df]
    masterDF = reduce(lambda left,right: pd.merge(left,right,on="MONTH_YEAR"), dfs)
    dataDict = {"ID":[],"BEST":[],"CB":[],"LVE":[]}
    
    #column_names = output1.columns
    #for i in range(1, len(column_names)):
    #        masterDF[column_names[i]] = masterDF[column_names[i]].astype(float).round(2)
    
     # change currency conversion code for India
    # to get exchange rate from ccy_exch_rate
    # exrate_query = """select exch_rate from """+hana_db+""".ccy_exch_rate a
    #                 join """+hana_db+""".currency_type b on b.ccy_cd = a.from_ccy_cd and ctry_geo_id = ?  
    #                 where to_ccy_cd = 0"""
    
    exrate_query = """select exch_rate from """+hana_db+""".ccy_exch_rate a
                    join """+hana_db+""".currency_type b on b.ccy_cd = a.from_ccy_cd and ctry_geo_id = %s
                    where to_ccy_cd = 0"""
    
    input_data =(geo_id)
    cursor_data = connection.cursor()
    cursor_data.execute(exrate_query,input_data)
    rows1 = cursor_data.fetchmany(1)
    exch_rate = float(rows1[0][0]) 
    cursor_data.close()
            # converting into EUR
    
    
    for index, rows in masterDF.iterrows():
        year = str(rows['MONTH_YEAR'])
        #print(rows['BS_SALES_VOLUME'])
        dataDict["ID"].append(year+"_volume")
        dataDict["BEST"].append(rows['BS_SALES_VOLUME'])
        dataDict["CB"].append(rows['CS_SALES_VOLUME'])
        dataDict["LVE"].append(rows['LVE_SALES_VOLUME'])
        dataDict["ID"].append(year+"_value")
        dataDict["BEST"].append(rows['BS_SALES_VALUE'])
        dataDict["CB"].append(rows['CS_SALES_VALUE'])
        dataDict["LVE"].append(rows['LVE_SALES_VALUE'])
            
    updatedDF = pd.DataFrame(dataDict)
    
    dataDict1 = {"ID":[],"Best_pct_2019":[],"CB_pct_2019":[],"LVE_pct_2019":[],
                "Best_pct_2020":[],"CB_pct_2020":[],"LVE_pct_2020":[],
                "Best_pct_2021":[],"CB_pct_2021":[],"LVE_pct_2021":[],
                "BEST":[],"CB":[],"LVE":[]}
    
    ###################
    # 2020 volume
    ####################
    #dataDict1['Best_pct_2019'].append() 
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_volume','BEST'].values[0]
    
    dataDict1['ID'].append('2020_volume')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    dataDict1['Best_pct_2020'].append("NA")
    dataDict1['CB_pct_2020'].append("NA")
    dataDict1['LVE_pct_2020'].append("NA")
    dataDict1['Best_pct_2021'].append("NA")
    dataDict1['CB_pct_2021'].append("NA")
    dataDict1['LVE_pct_2021'].append("NA")
    
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2020_volume','BEST'].values[0])/1000000,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2020_volume','CB'].values[0])/1000000,2)
    dataDict1['CB'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2020_volume','LVE'].values[0])/1000000,2)
    dataDict1['LVE'].append(value2)
    
    
    ###################
    # 2020 value
    ####################
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_value','BEST'].values[0]
    
    dataDict1['ID'].append('2020_value')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2020_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    dataDict1['Best_pct_2020'].append("NA")
    dataDict1['CB_pct_2020'].append("NA")
    dataDict1['LVE_pct_2020'].append("NA")
    dataDict1['Best_pct_2021'].append("NA")
    dataDict1['CB_pct_2021'].append("NA")
    dataDict1['LVE_pct_2021'].append("NA")
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2020_value','BEST'].values[0])/1000000)*exch_rate,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2020_value','CB'].values[0])/1000000)*exch_rate,2)
    dataDict1['CB'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2020_value','LVE'].values[0])/1000000)*exch_rate,2)
    dataDict1['LVE'].append(value2)
    
    
    ###################
    # 2021 volume
    ####################
    #dataDict1['Best_pct_2019'].append() 
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','BEST'].values[0]
    
    dataDict1['ID'].append('2021_volume')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2020'].append(str(calc_value)+'%')
    
    dataDict1['Best_pct_2021'].append("NA")
    dataDict1['CB_pct_2021'].append("NA")
    dataDict1['LVE_pct_2021'].append("NA")
    
    
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2021_volume','BEST'].values[0])/1000000,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2021_volume','CB'].values[0])/1000000,2)
    dataDict1['CB'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2021_volume','LVE'].values[0])/1000000,2)
    dataDict1['LVE'].append(value2)
    
    ###################
    # 2021 value
    ####################
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','BEST'].values[0]
    
    dataDict1['ID'].append('2021_value')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2021_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2020'].append(str(calc_value)+'%')
    
    dataDict1['Best_pct_2021'].append("NA")
    dataDict1['CB_pct_2021'].append("NA")
    dataDict1['LVE_pct_2021'].append("NA")
    
    
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2021_value','BEST'].values[0])/1000000)*exch_rate,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2021_value','CB'].values[0])/1000000)*exch_rate,2)
    dataDict1['CB'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2021_value','LVE'].values[0])/1000000)*exch_rate,2)
    dataDict1['LVE'].append(value2)
    
    
    ###################
    # 2022 volume
    ####################
    #dataDict1['Best_pct_2019'].append() 
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','BEST'].values[0]
    
    dataDict1['ID'].append('2022_volume')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_volume','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2021'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_volume','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2021'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_volume','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_volume','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2021'].append(str(calc_value)+'%')
    
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2022_volume','BEST'].values[0])/1000000,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2022_volume','CB'].values[0])/1000000,2)
    dataDict1['CB'].append(value2)
    value2 = round(float(updatedDF.loc[updatedDF['ID']=='2022_volume','LVE'].values[0])/1000000,2)
    dataDict1['LVE'].append(value2)
    
    ###################
    # 2021 value
    ####################
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','BEST'].values[0]
    
    dataDict1['ID'].append('2022_value')
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2019_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2019'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2020'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2020_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2020'].append(str(calc_value)+'%')
    
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_value','BEST'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','BEST'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['Best_pct_2021'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_value','CB'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','CB'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['CB_pct_2021'].append(str(calc_value)+'%')
    
    value1 =  updatedDF.loc[updatedDF['ID']=='2021_value','LVE'].values[0]
    value2 = updatedDF.loc[updatedDF['ID']=='2022_value','LVE'].values[0]
    calc_value = round(((value2-value1)/value1)*100,2)
    dataDict1['LVE_pct_2021'].append(str(calc_value)+'%')
    
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2022_value','BEST'].values[0])/1000000)*exch_rate,2)
    dataDict1['BEST'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2022_value','CB'].values[0])/1000000)*exch_rate,2)
    dataDict1['CB'].append(value2)
    value2 = round(float((updatedDF.loc[updatedDF['ID']=='2022_value','LVE'].values[0])/1000000)*exch_rate,2)
    dataDict1['LVE'].append(value2)
    
    
    updatedDF2 = pd.DataFrame(dataDict1)
    
    return updatedDF2


def ul_db_ppt_sub_category_list(geo_id):
   

    select_query3 = """select subcat_desc from 
                        """+hana_db+""".subcat_type  where  geo_id = %s"""

    input_values3 = (geo_id)
    
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values3)
    rows = cursor_data.fetchall()
    column_names2 = ["SUB_CATEGORY_NAME"]
    format_perf_DF = pd.DataFrame(rows,columns=column_names2)
    cursor_data.close()
    
    return format_perf_DF


def ul_db_ppt_actual_data_source(geo_id,division,catg_cd,sub_catg_cd):
   

    # select_query3 = """select SUBSTRING(MONTHNAME(min(a.period_ending_date)),0,3) ||' '|| YEAR(min(a.period_ending_date)) || ' - ' ||
    #                     SUBSTRING(MONTHNAME(max(a.period_ending_date)),0,3) ||' '|| YEAR(max(a.period_ending_date)) as actual_date
    #                     FROM """+hana_db+""".res_recordset_save a
    #                     join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
    #                     join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
    #                     join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
    #                     where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
    #                     AND a.CATG_CD = %s
    #                     AND a.SUBCAT_CD = %s
    #                     AND a.SECTOR_SCENARIO_CD = 1
    #                     and a.CHNL_CD = 0
    #                     and a.FMT_CD = 0"""

    # SHIV CHANGE - CHANGE QUERY
    select_query3 = """select CONCAT(SUBSTRING(MONTHNAME(min(a.period_ending_date)),1,3),' ',YEAR(min(a.period_ending_date)),' - ',
       SUBSTRING(MONTHNAME(max(a.period_ending_date)),1,3),' ',YEAR(max(a.period_ending_date))) as actual_date
                        FROM """+hana_db+""".res_recordset_save a
                        join """+hana_db+""".category_type d on a.catg_cd = d.catg_cd and d.div_cd = %s
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = %s and b.catg_cd = d.catg_cd
                        join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = %s and ul_region_name = 'Country'
                        where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL'
                        AND a.CATG_CD = %s
                        AND a.SUBCAT_CD = %s
                        AND a.SECTOR_SCENARIO_CD = 1
                        and a.CHNL_CD = 0
                        and a.FMT_CD = 0"""

    input_values = (division,geo_id,geo_id,catg_cd,sub_catg_cd)
    
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values)
    rows = cursor_data.fetchall()
    print(str(rows))
    print(select_query3)
    column_names2 = ["DATA_SOURCE"]
    format_perf_DF = pd.DataFrame(rows,columns=column_names2)
    cursor_data.close()
    
    return format_perf_DF


def ul_db_ppt_metric_name(geo_id,division,catg_cd,sub_catg_cd):
   

    select_units = """ select vol_unit_desc from """+hana_db+""".vol_unit_type a
                        join """+hana_db+""".subcat_type b on a.vol_unit_cd = b.vol_unit_cd and  
                        geo_id= %s and CATG_CD = %s and SUBCAT_CD = %s """
        
    input_values = (geo_id,catg_cd,sub_catg_cd)
    cursor_data = connection.cursor()
    cursor_data.execute(select_units,input_values)
    rows = cursor_data.fetchmany(1)
    cursor_data.close()
    value_unit = rows[0][0]
    
    return value_unit


def ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd):
   

    roi_check = """ select ROI_CHANGE_FLAG from """+hana_db+""".subcat_type
                        where geo_id = %s
                        and catg_cd = %s
                        and subcat_cd = %s """
        
    input_values = (geo_id,catg_cd,sub_catg_cd)
    cursor_data = connection.cursor()
    cursor_data.execute(roi_check,input_values)
    rows = cursor_data.fetchmany(1)
    cursor_data.close()
    roi_flag = int(rows[0][0])
    
    return roi_flag







##request_header = {"COUNTRY_NAME": "142", "DIVISION": "1", "CATAGORY_NAME": "9", "SUB_CATEGORY_NAME": "1"}
#Retail_DF = ul_db_ppt_channel_pct_contrib_comparison("73","2","5","8","1")
#Retail_DF = ul_db_ppt_region_pct_contrib_comparison("142","1","8","1","5")
#Retail_DF = ul_db_ppt_forecast_vol("169","1","8","1")
#print(Retail_DF)
#output_DF = ul_db_ppt_forecast_vol("170","1","8","1")
#output_DF.to_csv("Brazil_deodorants.csv")