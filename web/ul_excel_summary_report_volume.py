# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 15:06:00 2021

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
    

# get sales data for all countries 
def get_global_sales_data(input_values):
    
    select_query = """select ctry_name, div_desc,catg_desc,subcat_desc, MONTH_YEAR,sum(SALES_VOLUME) from
                    (
                    select f.ctry_name,c.div_desc,e.catg_desc,b.subcat_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal))
                    as SALES_VOLUME
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
                    SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal))
                    as SALES_VOLUME
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





def ul_db_excel_summary_report_volume(parameters):

    
    # year config to generate excel summary report 
    pre_year1 = '2019'
    pre_year2 = '2020'
    curr_year = '2021'
    next_year = '2022'
    
    
    column_names = ["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","MONTH_YEAR","VOLUME"]
    
    # to get sales data for Baseline scenario
    
    input_values =(pre_year1,pre_year2,curr_year,next_year,"1",pre_year1,pre_year2,curr_year,next_year)
    baseline_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
    
    if baseline_DF.shape[0] <1:
        responseData ={"request_header":parameters,
                       "EXCEL_DATA":""
                         }
            
        json_object = json.dumps(responseData)
        return json_object

    
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
    baseline_DF["VOLUME"] = baseline_DF["VOLUME"].astype(float)
    baseline_DF["VOLUME"] = baseline_DF["VOLUME"]/1000
    
    longcovid_DF_new["VOLUME"] = longcovid_DF_new["VOLUME"].astype(float)
    longcovid_DF_new["VOLUME"] = longcovid_DF_new["VOLUME"]/1000
    
    roi_DF_new["VOLUME"] = roi_DF_new["VOLUME"].astype(float)
    roi_DF_new["VOLUME"] = roi_DF_new["VOLUME"]/1000
    
    
    baselinep_DF = pd.pivot_table(baseline_DF,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VOLUME')
    longcovidp_DF = pd.pivot_table(longcovid_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VOLUME')
    roip_DF = pd.pivot_table(roi_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY"],columns='MONTH_YEAR',values='VOLUME')
    
    
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
    combined_DF.reset_index(inplace=True)
 
    
    # add Nutrition cell adding Nutrition-adults/kids/toddler
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
                   
    
    
    #final_DF2 = final_DF.copy()
  
   # calculate global total values  
    tempDF6 = final_DF[(final_DF['CATEGORY']=='Total') & (final_DF['SUB_CATEGORY']=='Total') & (final_DF['DIVISION']=='BPC') ].copy()   
    df_len = max(final_DF.index)+1
    final_DF.loc[df_len] = tempDF6.sum(numeric_only=True, axis=0) 
    final_DF.at[df_len, 'COUNTRY'] = "Global"
    final_DF.at[df_len, 'DIVISION'] = "BPC"
    final_DF.at[df_len, 'CATEGORY'] = "Total"
    final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    tempDF7 = final_DF[(final_DF['CATEGORY']=='Total') & (final_DF['SUB_CATEGORY']=='Total') & (final_DF['DIVISION']=='F&R') ].copy()   
    df_len = max(final_DF.index)+1
    final_DF.loc[df_len] = tempDF7.sum(numeric_only=True, axis=0) 
    final_DF.at[df_len, 'COUNTRY'] = "Global"
    final_DF.at[df_len, 'DIVISION'] = "F&R"
    final_DF.at[df_len, 'CATEGORY'] = "Total"
    final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
    
    tempDF8 = final_DF[(final_DF['CATEGORY']=='Total') & (final_DF['SUB_CATEGORY']=='Total') & (final_DF['DIVISION']=='HC') ].copy()   
    df_len = max(final_DF.index)+1
    final_DF.loc[df_len] = tempDF8.sum(numeric_only=True, axis=0) 
    final_DF.at[df_len, 'COUNTRY'] = "Global"
    final_DF.at[df_len, 'DIVISION'] = "HC"
    final_DF.at[df_len, 'CATEGORY'] = "Total"
    final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
    
#    rows = final_DF.loc[(final_DF['COUNTRY']=='Global') &(final_DF['DIVISION']=='BPC'),:]
#    final_DF = final_DF.append(rows, ignore_index=True)
#    
#    rows = final_DF.loc[(final_DF['COUNTRY']=='Global') &(final_DF['DIVISION']=='F&R'),:]
#    final_DF = final_DF.append(rows, ignore_index=True)
#    
#    rows = final_DF.loc[(final_DF['COUNTRY']=='Global') &(final_DF['DIVISION']=='HC'),:]
#    final_DF = final_DF.append(rows, ignore_index=True)
    
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
    final_DF["BS_H1_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]))-1)*100
    final_DF["BS_H2_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    final_DF["BS_FY_"+curr_year +"_"+pre_year2] = (((final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]+final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF["BS_FY_"+ next_year +"_"+pre_year1] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF["BS_FY_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"] + final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
    
    
    # capture long covid growth numbers 
    final_DF["LC_H1_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]))-1)*100
    final_DF["LC_H2_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    final_DF["LC_FY_"+curr_year +"_"+pre_year2] = (((final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]+final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF["LC_FY_"+ next_year +"_"+pre_year1] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF["LC_FY_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"] + final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
    
    # capture ROI scenario growth numbers 
    final_DF["RI_H1_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]))-1)*100
    final_DF["RI_H2_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    final_DF["RI_FY_"+curr_year +"_"+pre_year2] = (((final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]+final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
    final_DF["RI_FY_"+ next_year +"_"+pre_year1] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
    final_DF["RI_FY_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"] + final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
    
    final_DF.replace({-100:0},inplace=True)
    
    
    colnames = final_DF.columns
    final_DF[colnames[4:]] = final_DF[colnames[4:]].round(2)
    
    #final_DF.to_csv("sample_summary2_volume_final.csv")
    
    responseDict2 = final_DF.to_dict(orient='records')

    responseData ={"request_header":parameters,
                   "EXCEL_DATA":responseDict2
                     }
        
    json_object = json.dumps(responseData)
    return json_object


# unit testing
#input_vaiables= {"COUNTRY":"all"}
#output1 = ul_db_excel_summary_report_volume(input_vaiables)
#print(output1)
#    