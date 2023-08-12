# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 15:06:00 2021

@author: Rameshkumar
"""


import pandas as pd
# from hdbcli import dbapi
import numpy as np
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

# get sales data for all countries 
def get_global_sales_data(input_values):
    
    select_query = """select ctry_name, div_desc,catg_desc,subcat_desc, MONTH_YEAR,sum(SALES_VALUE) from
                    (
                    select f.ctry_name,c.div_desc,e.catg_desc,b.subcat_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)*
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal))
                    as SALES_VALUE
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id  and  z.ul_region_name = 'Country'
                    join """+hana_db+""".geo_hier f on z.ctry_geo_id = f.geo_id  
                    join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd
                    join """+hana_db+""".division_type c on c.div_cd = e.div_cd
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = z.ctry_geo_id and b.catg_cd = e.catg_cd
                    join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                    where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL' 
                    and a.chnl_cd = 0
                    and a.fmt_cd =0
                    and a.SECTOR_SCENARIO_CD = 1
                    and year(a.period_ending_date) in(%s,%s,%s,%s)
                    group by b.subcat_desc,e.catg_desc,f.ctry_name,c.div_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                    union
                    select f.ctry_name,c.div_desc,e.catg_desc,b.subcat_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)*
                    cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal))
                    as SALES_VALUE
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id  and  z.ul_region_name = 'Country'
                    join """+hana_db+""".geo_hier f on z.ctry_geo_id = f.geo_id  
                    join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd
                    join """+hana_db+""".division_type c on c.div_cd = e.div_cd
                    join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = z.ctry_geo_id and b.catg_cd = e.catg_cd
                    join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD
                    where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST' 
                    and a.chnl_cd = 0
                    and a.fmt_cd =0
                    and a.SECTOR_SCENARIO_CD = %s
                    and year(a.period_ending_date) in(%s,%s,%s,%s)
                    group by b.subcat_desc,e.catg_desc,f.ctry_name,c.div_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE))
                    ) sub
                    group by MONTH_YEAR,ctry_name, div_desc,catg_desc, subcat_desc
                    order by ctry_name,div_desc,subcat_desc,MONTH_YEAR
                    """
    
   
    cursor_data = connection.cursor()
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


# to get UL TURNOVER_NUMBER
def get_ul_turnover_data():
    
    select_query = """select f.ctry_name,c.div_desc,e.catg_desc,b.subcat_desc,a.turnover_euro_mill
                        from """+hana_db+""".subcat_turnover a
                        join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = a.geo_id 
                        join """+hana_db+""".geo_hier f on a.geo_id = f.geo_id  
                        join """+hana_db+""".category_type e on b.catg_cd = e.catg_cd
                        join """+hana_db+""".division_type c on c.div_cd = e.div_cd
                        order by f.ctry_name,c.div_desc,b.subcat_desc """
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    return rows


# currecy conversion based on conversion rate in table 
def currenty_conversion(tempDF,country):
    
    # to get exchange rate from DB
    select_query = """ select exch_rate from
                        """+hana_db+""".ccy_exch_rate
                        where to_ccy_cd = 0
                        and from_ccy_cd = (select ccy_cd from """+hana_db+""".currency_type
                        where ctry_geo_id = (select GEO_ID from """+hana_db+""".geo_hier 
                        where ctry_name =%s
                        and region_name ='DUMMY'
                        and city_name ='DUMMY')) """
   
    cursor_data = connection.cursor()
    input_values = (country)
    cursor_data.execute(select_query,input_values)
    rows = cursor_data.fetchall()
    cursor_data.close()
    #rows = cursor_data.fetchmany(5)
    exch_rate = float(rows[0][0])
    col_names = tempDF.columns
    for i in range (4,len(col_names)):
        tempDF[col_names[i]] = round((tempDF[col_names[i]]/1000000)*exch_rate,2)

    return tempDF

def calc_unweighted_growth_rate(dataDF,pre_year1,pre_year2,curr_year,next_year):
        
    combined_DF = dataDF.copy()
    final_DF = pd.DataFrame()
    country_list = list(combined_DF['COUNTRY'].unique())
    #country_list = ['France']
    #country = 'India'
    for country in country_list:
        tempDF = combined_DF[combined_DF['COUNTRY']==country].copy()
        tempDF.fillna(0,inplace =True)
        #tempDF = currenty_conversion(tempDF,country)
        if tempDF.shape[0] > 1:
            div_list = list(tempDF['DIVISION'].unique())
            #div_name ='BPC'
            for div_name in div_list:
                tempDF2 = tempDF[tempDF['DIVISION']==div_name].copy()
                if tempDF2. shape[0] >1:
                    category_list = list(tempDF2['CATEGORY'].unique())
                    #catg_name = 'Hair Care'
                    for catg_name in category_list:
                        tempDF3 = tempDF2[tempDF2['CATEGORY']==catg_name].copy()
                        if tempDF3.shape[0] >1:
                            df_len = max(tempDF3.index)+1
                            tempDF3.loc[df_len] = tempDF3.sum(numeric_only=True, axis=0) 
                            tempDF3.at[df_len, 'COUNTRY'] = country
                            tempDF3.at[df_len, 'DIVISION'] = div_name
                            tempDF3.at[df_len, 'CATEGORY'] = catg_name
                            tempDF3.at[df_len, 'SUB_CATEGORY'] = "Total"
                            if final_DF.shape[0] < 1:
                                final_DF = tempDF3.copy()
                            else:
                                final_DF = final_DF.append(tempDF3)
                        else:
                            if final_DF.shape[0] < 1:
                                final_DF = tempDF3.copy()
                            else:
                                final_DF = final_DF.append(tempDF3)
                else:
                    if final_DF.shape[0] < 1:
                        final_DF = tempDF2.copy()
                    else:
                        final_DF = final_DF.append(tempDF2)
            for div_name in div_list:
                tempDF4 = tempDF[tempDF['DIVISION']==div_name].copy()
                if tempDF4. shape[0] >1:
                    df_len = max(final_DF.index)+1
                    final_DF.loc[df_len] = tempDF4.sum(numeric_only=True, axis=0) 
                    final_DF.at[df_len, 'COUNTRY'] = country
                    final_DF.at[df_len, 'DIVISION'] = div_name
                    final_DF.at[df_len, 'CATEGORY'] = "Total"
                    final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
                
                else:
                    df_len = max(tempDF4.index)
                    tempDF4.at[df_len, 'COUNTRY'] = country
                    tempDF4.at[df_len, 'DIVISION'] = div_name
                    tempDF4.at[df_len, 'CATEGORY'] = "Total"
                    tempDF4.at[df_len, 'SUB_CATEGORY'] = "Total"       
                   
                    if final_DF.shape[0] < 1:
                        final_DF = tempDF4.copy()
                    else:
                        final_DF = final_DF.append(tempDF4)
        
        else:
        
            if final_DF.shape[0] < 1:
                final_DF = tempDF.copy()
            else:
                final_DF = final_DF.append(tempDF)
            
            df_len = max(final_DF.index)+1
            div_name = list(tempDF['DIVISION'].unique())
            final_DF.loc[df_len] = tempDF.sum(numeric_only=True, axis=0) 
            final_DF.at[df_len, 'COUNTRY'] = country
            final_DF.at[df_len, 'DIVISION'] = div_name[0]
            final_DF.at[df_len, 'CATEGORY'] = "Total"
            final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
        
        
        if tempDF.shape[0] >=1:
            df_len = max(final_DF.index)+1
            final_DF.loc[df_len] = tempDF.sum(numeric_only=True, axis=0) 
            final_DF.at[df_len, 'COUNTRY'] = country
            final_DF.at[df_len, 'DIVISION'] = "Total"
            final_DF.at[df_len, 'CATEGORY'] = "Total"
            final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"        
                   
    
    
    # convery to EURO based on country exchange rate
    final_DF2 = pd.DataFrame()
    for country in country_list:
        tempDF = final_DF[final_DF['COUNTRY']==country].copy()
        tempDF = currenty_conversion(tempDF,country)
        if final_DF2.shape[0] < 1:
            final_DF2 = tempDF.copy()
        else:
            final_DF2 = final_DF2.append(tempDF,ignore_index=True)
    
  
   # calculate global total values  
    tempDF6 = final_DF2[(final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='BPC') ].copy()   
    df_len = max(final_DF2.index)+1
    final_DF2.loc[df_len] = tempDF6.sum(numeric_only=True, axis=0) 
    final_DF2.at[df_len, 'COUNTRY'] = "Global"
    final_DF2.at[df_len, 'DIVISION'] = "BPC"
    final_DF2.at[df_len, 'CATEGORY'] = "Total"
    final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    tempDF7 = final_DF2[(final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='F&R') ].copy()   
    df_len = max(final_DF2.index)+1
    final_DF2.loc[df_len] = tempDF7.sum(numeric_only=True, axis=0) 
    final_DF2.at[df_len, 'COUNTRY'] = "Global"
    final_DF2.at[df_len, 'DIVISION'] = "F&R"
    final_DF2.at[df_len, 'CATEGORY'] = "Total"
    final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    tempDF8 = final_DF2[(final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='HC') ].copy()   
    df_len = max(final_DF2.index)+1
    final_DF2.loc[df_len] = tempDF8.sum(numeric_only=True, axis=0) 
    final_DF2.at[df_len, 'COUNTRY'] = "Global"
    final_DF2.at[df_len, 'DIVISION'] = "HC"
    final_DF2.at[df_len, 'CATEGORY'] = "Total"
    final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='BPC'),:]
    final_DF = final_DF.append(rows, ignore_index=True)
    
    rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='F&R'),:]
    final_DF = final_DF.append(rows, ignore_index=True)
    
    rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='HC'),:]
    final_DF = final_DF.append(rows, ignore_index=True)
    
    
    
    # capture baseline scenario growth numbers 
    final_DF2["UW_BS_H1_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]))-1)*100
    final_DF2["UW_BS_H2_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    final_DF2["UW_BS_FY_"+curr_year +"_"+pre_year2] = (((final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]+final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["UW_BS_FY_"+ next_year +"_"+pre_year1] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["UW_BS_FY_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"] + final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    
    
    # capture long covid growth numbers 
    final_DF2["UW_LC_H1_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]))-1)*100
    final_DF2["UW_LC_H2_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    final_DF2["UW_LC_FY_"+curr_year +"_"+pre_year2] = (((final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]+final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["UW_LC_FY_"+ next_year +"_"+pre_year1] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["UW_LC_FY_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"] + final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    
    # capture ROI scenario growth numbers 
    final_DF2["UW_RI_H1_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]))-1)*100
    final_DF2["UW_RI_H2_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    final_DF2["UW_RI_FY_"+curr_year +"_"+pre_year2] = (((final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]+final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["UW_RI_FY_"+ next_year +"_"+pre_year1] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["UW_RI_FY_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"] + final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    
    final_DF2.replace({-100:0},inplace=True)
    final_DF2.set_index(["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],inplace=True)
    cols_list = list(final_DF2.columns)
    growth_rate_cols = cols_list[-15:]
    final_DF2 = final_DF2[growth_rate_cols]
    return final_DF2


def ul_db_excel_summary_report(parameters):
    
    # year config to generate excel summary report 
    pre_year1 = '2019'
    pre_year2 = '2020'
    curr_year = '2021'
    next_year = '2022'
    
    
    column_names = ["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","MONTH_YEAR","VALUE"]
    
    # to get sales data for Baseline scenario
    
    input_values =(pre_year1,pre_year2,curr_year,next_year,"1",pre_year1,pre_year2,curr_year,next_year)
    baseline_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
    
    
    if baseline_DF.shape[0] <1:
        responseData ={"request_header":parameters,
                       "EXCEL_DATA":""
                         }
            
        json_object = json.dumps(responseData)
        return json_object
    
    
    column_names2 = ["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","UL_TO_YTD_NOV"]
    ultoDF =  pd.DataFrame(get_ul_turnover_data(),columns=column_names2)
    
    
    ultoDF.set_index(["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],inplace=True)
    ultoDF["UL_TO_YTD_NOV"] = ultoDF["UL_TO_YTD_NOV"].astype(float)
    #ultoDF.replace(0,1,inplace=False)
    #ultoDF.UL_TO_YTD_NOV.replace(to_replace=0, value=0.000001, inplace=True)
    
    
    # to get sales data for Long Covid scenario 
    input_values =(pre_year1,pre_year2,curr_year,next_year,"4",pre_year1,pre_year2,curr_year,next_year)
    longcovid_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
    
    # to get sales data for Return of inflation scenario 
    input_values =(pre_year1,pre_year2,curr_year,next_year,"5",pre_year1,pre_year2,curr_year,next_year)
    roi_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
    
    
    # filter 2021 and 2022
    longcovid_DF_new = longcovid_DF[(longcovid_DF['MONTH_YEAR'].str.contains(curr_year)) | (longcovid_DF['MONTH_YEAR'].str.contains(next_year))].copy()
    roi_DF_new = roi_DF[(roi_DF['MONTH_YEAR'].str.contains(curr_year)) | (roi_DF['MONTH_YEAR'].str.contains(next_year))].copy()
    
    
    # pivot tables 
    baseline_DF["VALUE"] = baseline_DF["VALUE"].astype(float)
    longcovid_DF_new["VALUE"] = longcovid_DF_new["VALUE"].astype(float)
    roi_DF_new["VALUE"] = roi_DF_new["VALUE"].astype(float)
    
    baselinep_DF = pd.pivot_table(baseline_DF,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VALUE')
    longcovidp_DF = pd.pivot_table(longcovid_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VALUE')
    roip_DF = pd.pivot_table(roi_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VALUE')
    
    
    #rename columns
    baselinep_DF.rename(columns={pre_year1+"-Q1":"A_"+pre_year1+"_Q1",pre_year1+"-Q2":"A_"+pre_year1+"_Q2",pre_year1+"-Q3":"A_"+pre_year1+"_Q3",pre_year1+"-Q4":"A_"+pre_year1+"_Q4",
                                 pre_year2 +"-Q1":"A_"+pre_year2 +"_Q1",pre_year2 +"-Q2":"A_"+pre_year2 +"_Q2",pre_year2 +"-Q3":"A_"+pre_year2 +"_Q3",pre_year2 +"-Q4":"A_"+pre_year2 +"_Q4",
                                 curr_year+"-Q1":"BS_"+curr_year +"_Q1",curr_year+"-Q2":"BS_"+curr_year +"_Q2",curr_year+"-Q3":"BS_"+curr_year +"_Q3",curr_year+"-Q4":"BS_"+curr_year +"_Q4",
                                 next_year+"-Q1":"BS_"+ next_year +"_Q1",next_year+"-Q2":"BS_"+ next_year +"_Q2",next_year+"-Q3":"BS_"+ next_year +"_Q3",next_year+"-Q4":"BS_"+ next_year +"_Q4",
                                 },inplace=True)
    
    baselinep_DF.insert(4,"A_"+pre_year1+"_FY", baselinep_DF["A_"+pre_year1+"_Q1"] + baselinep_DF["A_"+pre_year1+"_Q2"] +baselinep_DF["A_"+pre_year1+"_Q3"] +baselinep_DF["A_"+pre_year1+"_Q4"] )
    baselinep_DF.insert(9,"A_"+pre_year2 +"_FY", baselinep_DF["A_"+pre_year2 +"_Q1"] + baselinep_DF["A_"+pre_year2 +"_Q2"] +baselinep_DF["A_"+pre_year2 +"_Q3"] +baselinep_DF["A_"+pre_year2 +"_Q4"])
    
    longcovidp_DF.rename(columns={curr_year+"-Q1":"LC_"+curr_year +"_Q1",curr_year+"-Q2":"LC_"+curr_year +"_Q2",curr_year+"-Q3":"LC_"+curr_year +"_Q3",curr_year+"-Q4":"LC_"+curr_year +"_Q4",
                                 next_year+"-Q1":"LC_"+ next_year +"_Q1",next_year+"-Q2":"LC_"+ next_year +"_Q2",next_year+"-Q3":"LC_"+ next_year +"_Q3",next_year+"-Q4":"LC_"+ next_year +"_Q4",
                                 },inplace=True)
        
    roip_DF.rename(columns={curr_year+"-Q1":"RI_"+curr_year +"_Q1",curr_year+"-Q2":"RI_"+curr_year +"_Q2",curr_year+"-Q3":"RI_"+curr_year +"_Q3",curr_year+"-Q4":"RI_"+curr_year +"_Q4",
                                 next_year+"-Q1":"RI_"+ next_year +"_Q1",next_year+"-Q2":"RI_"+ next_year +"_Q2",next_year+"-Q3":"RI_"+ next_year +"_Q3",next_year+"-Q4":"RI_"+ next_year +"_Q4",
                                 },inplace=True)
    
    
        
    combined_DF = pd.merge(baselinep_DF, longcovidp_DF, left_index=True, right_index=True)
    combined_DF = pd.merge(combined_DF, roip_DF, left_index=True, right_index=True)
    combined_DF = pd.merge(combined_DF, ultoDF, left_index=True, right_index=True)
    
    combined_DF.reset_index(inplace=True)
    
    india_food_tempDP = combined_DF[(combined_DF['COUNTRY']=='India') & ((combined_DF['CATEGORY']=='Food'))].copy()
    df_len = max(india_food_tempDP.index)+1
    india_food_tempDP.loc[df_len] = india_food_tempDP.sum(numeric_only=True, axis=0) 
    india_food_tempDP.at[df_len, 'COUNTRY'] = "India"
    india_food_tempDP.at[df_len, 'DIVISION'] = "F&R"
    india_food_tempDP.at[df_len, 'CATEGORY'] = "Food"
    india_food_tempDP.at[df_len, 'SUB_CATEGORY'] = "Nutrition"
    combined_DF = combined_DF.append(india_food_tempDP.loc[df_len], ignore_index=True)
    combined_DF.sort_values(by=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],inplace=True)
    combined_DF.reset_index(drop=True,inplace=True)
    
    final_DF = combined_DF.copy()
   
    unweighted_df = calc_unweighted_growth_rate(final_DF,pre_year1,pre_year2,curr_year,next_year)
    #unweighted_df.to_csv("unweighted_df.csv")
    
    country_list = list(final_DF['COUNTRY'].unique())
    
    # convery to EURO based on country exchange rate
    final_DF2 = pd.DataFrame()
    for country in country_list:
        tempDF = final_DF[final_DF['COUNTRY']==country].copy()
        tempDF = currenty_conversion(tempDF,country)
        if final_DF2.shape[0] < 1:
            final_DF2 = tempDF.copy()
        else:
            final_DF2 = final_DF2.append(tempDF,ignore_index=True)
    
      
    
    # this is sample for calculation         
    #final_DF.fillna(0,inplace=True)
    
    #baselinep_DF1 = pd.pivot_table(baseline_DF,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VALUE').reset_index()
    #
    #masterDF = pd.DataFrame()
    #masterDF = baselinep_DF1[["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"]].copy()
    #masterDF["BS_H1_2022_2021"] = (((baselinep_DF1["2022-Q1"] + baselinep_DF1["2022-Q2"])/(baselinep_DF1["2021-Q1"] + baselinep_DF1["2021-Q2"]))-1)*100
    #masterDF["BS_H2_2022_2021"] = (((baselinep_DF1["2022-Q3"] + baselinep_DF1["2022-Q4"])/(baselinep_DF1["2021-Q3"] + baselinep_DF1["2021-Q4"]))-1)*100
    #masterDF["BS_FY_2021_2020"] = (((baselinep_DF1["2021-Q1"] + baselinep_DF1["2021-Q2"]+baselinep_DF1["2021-Q3"] + baselinep_DF1["2021-Q4"])/(baselinep_DF1["2020-Q1"] + baselinep_DF1["2020-Q2"] + baselinep_DF1["2020-Q3"] + baselinep_DF1["2020-Q4"]))-1)*100
    #masterDF["BS_FY_2022_2019"] = (((baselinep_DF1["2022-Q1"] + baselinep_DF1["2022-Q2"]+baselinep_DF1["2022-Q3"] + baselinep_DF1["2022-Q4"])/(baselinep_DF1["2019-Q1"] + baselinep_DF1["2019-Q2"] + baselinep_DF1["2019-Q3"] + baselinep_DF1["2019-Q4"]))-1)*100
    #masterDF["BS_FY_2022_2021"] = (((baselinep_DF1["2022-Q1"] + baselinep_DF1["2022-Q2"]+baselinep_DF1["2022-Q3"] + baselinep_DF1["2022-Q4"])/(baselinep_DF1["2021-Q1"] + baselinep_DF1["2021-Q2"] + baselinep_DF1["2021-Q3"] + baselinep_DF1["2021-Q4"]))-1)*100
    #
    #masterDF.to_csv("sample_summary1.csv")
    
    
    
    # capture baseline scenario growth numbers 
    final_DF2["BS_H1_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]))-1)*100
    final_DF2["BS_H2_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    final_DF2["BS_FY_"+curr_year +"_"+pre_year2] = (((final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]+final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["BS_FY_"+ next_year +"_"+pre_year1] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["BS_FY_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"] + final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    
    
    # capture long covid growth numbers 
    final_DF2["LC_H1_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]))-1)*100
    final_DF2["LC_H2_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    final_DF2["LC_FY_"+curr_year +"_"+pre_year2] = (((final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]+final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["LC_FY_"+ next_year +"_"+pre_year1] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["LC_FY_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"] + final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    
    # capture ROI scenario growth numbers 
    final_DF2["RI_H1_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]))-1)*100
    final_DF2["RI_H2_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    final_DF2["RI_FY_"+curr_year +"_"+pre_year2] = (((final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]+final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF2["RI_FY_"+ next_year +"_"+pre_year1] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF2["RI_FY_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"] + final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    
    final_DF2.replace({-100:0},inplace=True)
    #final_DF2.to_csv("sample_summary2_final.csv")
    

    colnames = final_DF2.columns
    
    cols_list = list(final_DF2.columns) 
    growth_rate_cols = cols_list[39:]
    final_DF2[colnames[4:]] = final_DF2[colnames[4:]].round(2)
    final_DF2['UL_TO_YTD_NOV'] = final_DF['UL_TO_YTD_NOV']
    
    #final_DF2.to_csv("sample_summary2_final.csv")
    
    weight_final_DF = pd.DataFrame()
    
    #country_list = ['France']
    #country = 'India'
    for country in country_list:
        tempDF = final_DF2[final_DF2['COUNTRY']==country].copy()
        tempDF.fillna(0,inplace =True)
        #tempDF = currenty_conversion(tempDF,country)
        if tempDF.shape[0] > 1:
            div_list = list(tempDF['DIVISION'].unique())
            #div_name ='BPC'
            for div_name in div_list:
                tempDF2 = tempDF[tempDF['DIVISION']==div_name].copy()
                if tempDF2. shape[0] >1:
                    category_list = list(tempDF2['CATEGORY'].unique())
                    #catg_name = 'Hair Care'
                    for catg_name in category_list:
                        tempDF3 = tempDF2[tempDF2['CATEGORY']==catg_name].copy()
                        if tempDF3.shape[0] >1:
                            # check UL TO number is available at category level 
                            for col_name in growth_rate_cols:
                                tempDF3[col_name] = (tempDF3[col_name] * tempDF3['UL_TO_YTD_NOV'])/100
                                    
                            df_len = max(tempDF3.index)+1
                            tempDF3.loc[df_len] = tempDF3.sum(numeric_only=True, axis=0) 
                            tempDF3.at[df_len, 'COUNTRY'] = country
                            tempDF3.at[df_len, 'DIVISION'] = div_name
                            tempDF3.at[df_len, 'CATEGORY'] = catg_name
                            tempDF3.at[df_len, 'SUB_CATEGORY'] = "Total"
                            
                            for col_name in growth_rate_cols:
                                tempDF3[col_name] = (tempDF3[col_name]/tempDF3['UL_TO_YTD_NOV'])*100
                            
                            if weight_final_DF.shape[0] < 1:
                                weight_final_DF = tempDF3.copy()
                            else:
                                weight_final_DF = weight_final_DF.append(tempDF3)
                                                    
                        else:
                            if weight_final_DF.shape[0] < 1:
                                weight_final_DF = tempDF3.copy()
                            else:
                                weight_final_DF = weight_final_DF.append(tempDF3)
                else:
                    if weight_final_DF.shape[0] < 1:
                        weight_final_DF = tempDF2.copy()
                    else:
                        weight_final_DF = weight_final_DF.append(tempDF2)
            for div_name in div_list:
                tempDF4 = tempDF[tempDF['DIVISION']==div_name].copy()
                if tempDF4. shape[0] >1:
                    # to check UL TO number at divison level 
                    for col_name in growth_rate_cols:
                        tempDF4[col_name] = (tempDF4[col_name] * tempDF4['UL_TO_YTD_NOV'])/100
                            
                    df_len = max(tempDF4.index)+1
                    tempDF4.loc[df_len] = tempDF4.sum(numeric_only=True, axis=0) 
                    tempDF4.at[df_len, 'COUNTRY'] = country
                    tempDF4.at[df_len, 'DIVISION'] = div_name
                    tempDF4.at[df_len, 'CATEGORY'] = "Total"
                    tempDF4.at[df_len, 'SUB_CATEGORY'] = "Total"
                    for col_name in growth_rate_cols:
                        tempDF4[col_name] = (tempDF4[col_name]/tempDF4['UL_TO_YTD_NOV'])*100
                    
                    df_len2 = max(weight_final_DF.index)+1
                    weight_final_DF.loc[df_len2] = tempDF4.loc[df_len]
                
                
                else:
                    df_len = max(tempDF4.index)
                    tempDF4.at[df_len, 'COUNTRY'] = country
                    tempDF4.at[df_len, 'DIVISION'] = div_name
                    tempDF4.at[df_len, 'CATEGORY'] = "Total"
                    tempDF4.at[df_len, 'SUB_CATEGORY'] = "Total"       
                   
                    if weight_final_DF.shape[0] < 1:
                        weight_final_DF = tempDF4.copy()
                    else:
                        weight_final_DF = weight_final_DF.append(tempDF4)
        
        else:
        
            if weight_final_DF.shape[0] < 1:
                weight_final_DF = tempDF.copy()
            else:
                weight_final_DF = weight_final_DF.append(tempDF)
            
            df_len = max(weight_final_DF.index)+1
            div_name = list(tempDF['DIVISION'].unique())
            weight_final_DF.loc[df_len] = tempDF.sum(numeric_only=True, axis=0) 
            weight_final_DF.at[df_len, 'COUNTRY'] = country
            weight_final_DF.at[df_len, 'DIVISION'] = div_name[0]
            weight_final_DF.at[df_len, 'CATEGORY'] = "Total"
            weight_final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
        
        
        if tempDF.shape[0] >=1:
            
            # to check UL TO number at country level 
            for col_name in growth_rate_cols:
                tempDF[col_name] = (tempDF[col_name] * tempDF['UL_TO_YTD_NOV'])/100
                    
            df_len = max(tempDF.index)+1
            tempDF.loc[df_len] = tempDF.sum(numeric_only=True, axis=0) 
            tempDF.at[df_len, 'COUNTRY'] = country
            tempDF.at[df_len, 'DIVISION'] = "Total"
            tempDF.at[df_len, 'CATEGORY'] = "Total"
            tempDF.at[df_len, 'SUB_CATEGORY'] = "Total"        
            for col_name in growth_rate_cols:
                tempDF[col_name] = (tempDF[col_name]/tempDF['UL_TO_YTD_NOV'])*100       
                
            df_len2 = max(weight_final_DF.index)+1
            weight_final_DF.loc[df_len2] = tempDF.loc[df_len]
            
    
    #calculate global total values  for BPC division 
    tempDF6 = weight_final_DF[(weight_final_DF['CATEGORY']=='Total') & (weight_final_DF['SUB_CATEGORY']=='Total') & (weight_final_DF['DIVISION']=='BPC') ].copy()   
    for col_name in growth_rate_cols:
        tempDF6[col_name] = (tempDF6[col_name] * tempDF6['UL_TO_YTD_NOV'])/100
    
    df_len = max(tempDF6.index)+1
    tempDF6.loc[df_len] = tempDF6.sum(numeric_only=True, axis=0) 
    tempDF6.at[df_len, 'COUNTRY'] = "Global"
    tempDF6.at[df_len, 'DIVISION'] = "BPC"
    tempDF6.at[df_len, 'CATEGORY'] = "Total"
    tempDF6.at[df_len, 'SUB_CATEGORY'] = "Total"
    for col_name in growth_rate_cols:
        tempDF6[col_name] = (tempDF6[col_name]/tempDF6['UL_TO_YTD_NOV'])*100
     
    
    #calculate global total values  for F& R division                     
    
    tempDF7 = weight_final_DF[(weight_final_DF['CATEGORY']=='Total') & (weight_final_DF['SUB_CATEGORY']=='Total') & (weight_final_DF['DIVISION']=='F&R') ].copy()   
    for col_name in growth_rate_cols:
        tempDF7[col_name] = (tempDF7[col_name] * tempDF7['UL_TO_YTD_NOV'])/100
    
    
    df_len = max(tempDF7.index)+1
    tempDF7.loc[df_len] = tempDF7.sum(numeric_only=True, axis=0) 
    tempDF7.at[df_len, 'COUNTRY'] = "Global"
    tempDF7.at[df_len, 'DIVISION'] = "F&R"
    tempDF7.at[df_len, 'CATEGORY'] = "Total"
    tempDF7.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    for col_name in growth_rate_cols:
        tempDF7[col_name] = (tempDF7[col_name]/tempDF7['UL_TO_YTD_NOV'])*100
     
    
    #calculate global total values  for HC division                     
    
    tempDF8 = weight_final_DF[(weight_final_DF['CATEGORY']=='Total') & (weight_final_DF['SUB_CATEGORY']=='Total') & (weight_final_DF['DIVISION']=='HC') ].copy()   
    
    for col_name in growth_rate_cols:
        tempDF8[col_name] = (tempDF8[col_name] * tempDF8['UL_TO_YTD_NOV'])/100
    
    
    df_len = max(tempDF8.index)+1
    tempDF8.loc[df_len] = tempDF8.sum(numeric_only=True, axis=0) 
    tempDF8.at[df_len, 'COUNTRY'] = "Global"
    tempDF8.at[df_len, 'DIVISION'] = "HC"
    tempDF8.at[df_len, 'CATEGORY'] = "Total"
    tempDF8.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    for col_name in growth_rate_cols:
        tempDF8[col_name] = (tempDF8[col_name]/tempDF8['UL_TO_YTD_NOV'])*100
     
    
    
    rows = tempDF6.loc[(tempDF6['COUNTRY']=='Global') &(tempDF6['DIVISION']=='BPC'),:]
    weight_final_DF = weight_final_DF.append(rows, ignore_index=True)
    
    rows = tempDF7.loc[(tempDF7['COUNTRY']=='Global') &(tempDF7['DIVISION']=='F&R'),:]
    weight_final_DF = weight_final_DF.append(rows, ignore_index=True)
    
    rows = tempDF8.loc[(tempDF8['COUNTRY']=='Global') &(tempDF8['DIVISION']=='HC'),:]
    weight_final_DF = weight_final_DF.append(rows, ignore_index=True)
    
    
    #weight_final_DF.to_csv("weighted_growth.csv")
    
    weight_final_DF.replace({-100:0},inplace=True)
    #final_DF2.to_csv("sample_summary2_final.csv")
    
    colnames = weight_final_DF.columns
    weight_final_DF[colnames[4:]] = weight_final_DF[colnames[4:]].round(2)
    #weight_final_DF.to_csv("weighted_df.csv")
    
    weight_final_DF.set_index(["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],inplace=True)
    unweighted_df = unweighted_df.round(2)

#    cols_list_tmp = list(weight_final_DF.columns)
#    cols_list1 = cols_list_tmp[:-15]
#    cols_list2 = cols_list_tmp[-15:]
#    final_tempDF = weight_final_DF[cols_list1].copy()
#    final_tempDF1 = weight_final_DF[cols_list2].copy()
#
#    total_DF1 = pd.merge(final_tempDF, unweighted_df, left_index=True, right_index=True)
#    total_DF = pd.merge(total_DF1, final_tempDF1, left_index=True, right_index=True)
#    total_DF.reset_index(inplace=True)

    total_DF = pd.merge(weight_final_DF, unweighted_df, left_index=True, right_index=True)
    total_DF.reset_index(inplace=True)


    # update weighted growth rate numbers to unweighted grwoth rate numbers when UL TO number is zerp    
    final_col_list = list(total_DF.columns)
    uw_cols = final_col_list[-30:-15]
    we_cols = final_col_list[-15:]
    for index,row in total_DF.iterrows():
        if row['UL_TO_YTD_NOV'] == 0:
            for i in range (0, len(uw_cols)):
               total_DF.at[index,we_cols[i]] = total_DF.at[index,uw_cols[i]]


    total_DF = total_DF.replace([np.inf, -np.inf], 0)
    #total_DF.to_csv("sample_summary2_final.csv")
    responseDict2 = total_DF.to_dict(orient='records')
    
    responseData ={"request_header":parameters,
                   "EXCEL_DATA":responseDict2
                     }
        
    json_object = json.dumps(responseData)
    return json_object


# unit testing
#input_vaiables= {"COUNTRY":"all"}
#tempDF = ul_db_excel_summary_report(input_vaiables)

