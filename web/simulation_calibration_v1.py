# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 12:58:45 2021

@author: Rameshkumar
"""

import pandas as pd
from sap_hana_credentials import connection

hana_db ='ULGROWTH20'

def ul_generic_region_calibration(total_df,regn_df,geo_id,catg_cd,sub_catg_cd):
    
    # formatting and getting total level data
    #total_sales_df['VOLUME'] = total_sales_df['VOLUME'].astype(float)
    #total_sales_df['VALUE'] = total_sales_df['VALUE'].astype(float)
    
    #fct_total_df = total_sales_df[total_sales_df['RECORD_TYPE']=='FORECAST']
    #fct_total_df['VOLUME'] = fct_total_df['VOLUME'] + (fct_total_df['VOLUME']*0.05)
    fct_total_df = total_df.copy()
    fct_total_df.set_index(['MONTH_YEAR'],inplace=True)
    
    #fct_total_df.to_csv("total_sales.csv")
    
    fct_regn_df = regn_df[regn_df['DATA_TYPE']=='FORECASTED']
    fct_regn_df_vol = pd.pivot_table(fct_regn_df,index=['MONTH_YEAR'],columns='REGION',values='VOLUME')
    fct_regn_df_val = pd.pivot_table(fct_regn_df,index=['MONTH_YEAR'],columns='REGION',values='VALUE')

    
    if str(geo_id)=='142' and str(sub_catg_cd) == '16':
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = ?
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = ?
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = ?
                        	AND aa.SUBCAT_CD = ?
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        and b.ul_region_name like '%_IC'
                        order by b.ul_region_name """
    
    
    
    elif str(geo_id)=='142':
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = ?
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = ?
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = ?
                        	AND aa.SUBCAT_CD = ?
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        and b.ul_region_name not like '%_IC'
                        order by b.ul_region_name """
    
    
    else:
    
        select_query3 = """select b.ul_region_name,c.ul_region_name
                        from """+hana_db+""".ul_region_map a
                        join """+hana_db+""".ul_geo_hier b on b.ul_rgn_geo_id = a.ul_rgn_geo_id and b.ctry_geo_id = ?
                        join """+hana_db+""".ul_geo_hier c on c.ul_rgn_geo_id = a.UL_PRMRY_RGN_GEO_ID and b.ctry_geo_id = ?
                        where a.ul_prmry_rgn_geo_id !=0
                        and a.UL_PRMRY_RGN_GEO_ID in 
                        (select distinct aa.ul_geo_id  
                        	from """+hana_db+""".res_recordset_save aa 
                        	join """+hana_db+""".ul_geo_hier z on aa.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and z.ul_region_name != 'Country'
                        	where aa.CATG_CD = ?
                        	AND aa.SUBCAT_CD = ?
                        	AND aa.SECTOR_SCENARIO_CD = 1)
                        order by b.ul_region_name """
                    
    
    
    input_values = (geo_id,geo_id,geo_id,catg_cd,sub_catg_cd)
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values)
    rows = cursor_data.fetchall()
    column_names2 = ["regn_desc","rollup_regn"]
    rollup_regn_DF = pd.DataFrame(rows,columns=column_names2)
    cursor_data.close()
    
    #fct_regn_df_vol = pd.pivot_table(fct_regn_df,index=['MONTH_YEAR'],columns='REGION',values='VOLUME')
    #fct_regn_df_val = pd.pivot_table(fct_regn_df,index=['MONTH_YEAR'],columns='REGION',values='VALUE')
    
    final_column_names = list(fct_regn_df_vol.columns)
    regn_column_names = list(fct_regn_df_vol.columns)
        
    if rollup_regn_DF.shape[0] > 1:
        rollup_regn_list = list(rollup_regn_DF['regn_desc'].unique())
    
        for region_name in rollup_regn_list:
            if region_name in regn_column_names:
                regn_column_names.remove(region_name)
        
        
        fct_regn_df_vol['region_total'] = fct_regn_df_vol[regn_column_names].sum(numeric_only=True, axis=1)
        fct_regn_df_val['region_total'] = fct_regn_df_val[regn_column_names].sum(numeric_only=True, axis=1)
        
        #fct_regn_df_vol.to_csv("region_before_calib.csv")
        
        for regn_names in regn_column_names:
            fct_regn_df_vol[regn_names] = fct_regn_df_vol[regn_names] *(fct_total_df['VOLUME']/fct_regn_df_vol['region_total'])
            fct_regn_df_val[regn_names] = fct_regn_df_val[regn_names] *(fct_total_df['VALUE']/fct_regn_df_val['region_total'])
        
        
        for roll_up_regn in rollup_regn_list:
            tempDF = rollup_regn_DF[rollup_regn_DF['regn_desc']==roll_up_regn]
            fct_regn_df_vol[roll_up_regn] =0
            fct_regn_df_val[roll_up_regn] =0
            col_names = list(tempDF['rollup_regn'].unique())
            for cols in col_names:
                fct_regn_df_vol[roll_up_regn] = fct_regn_df_vol[roll_up_regn] + fct_regn_df_vol[cols]
                fct_regn_df_val[roll_up_regn] = fct_regn_df_val[roll_up_regn] + fct_regn_df_val[cols]
    
    
    else:
        fct_regn_df_vol['region_total'] = fct_regn_df_vol.sum(numeric_only=True, axis=1)
        fct_regn_df_val['region_total'] = fct_regn_df_val.sum(numeric_only=True, axis=1)
        
        #fct_regn_df_vol.to_csv("region_before_calib.csv")
        
        for regn_names in final_column_names:
            fct_regn_df_vol[regn_names] = fct_regn_df_vol[regn_names] *(fct_total_df['VOLUME']/fct_regn_df_vol['region_total'])
            fct_regn_df_val[regn_names] = fct_regn_df_val[regn_names] *(fct_total_df['VALUE']/fct_regn_df_val['region_total'])
        
    
    
    #fct_regn_df_vol.to_csv("region_after_calib.csv")
    fct_regn_df_vol.reset_index(inplace=True)
    fct_regn_df_val.reset_index(inplace=True)
    
    
    fct_regn_df_vol_up = pd.melt(fct_regn_df_vol,id_vars=['MONTH_YEAR'],value_vars=final_column_names)
    fct_regn_df_vol_up.rename(columns = {"value":"VOLUME"},inplace=True)
    
    fct_regn_df_val_up = pd.melt(fct_regn_df_val,id_vars=['MONTH_YEAR'],value_vars=final_column_names)
    fct_regn_df_val_up.rename(columns = {"value":"VALUE"},inplace=True)
    
    final_df = fct_regn_df_vol_up.merge(fct_regn_df_val_up,on=['MONTH_YEAR','REGION'])
    final_df.dropna(subset=['VOLUME', 'VALUE'],inplace=True)
    final_df["DATA_TYPE"] = 'FORECASTED'

    return final_df



def ul_generic_format_calibration(total_df,fmt_df):
    
    # formatting and getting total level data
    #total_sales_df['VOLUME'] = total_sales_df['VOLUME'].astype(float)
    #total_sales_df['VALUE'] = total_sales_df['VALUE'].astype(float)
    
    #fct_total_df = total_sales_df[total_sales_df['RECORD_TYPE']=='FORECAST']
    #fct_total_df['VOLUME'] = fct_total_df['VOLUME'] + (fct_total_df['VOLUME']*0.05)
    fct_total_df = total_df.copy()
    fct_total_df.set_index(['MONTH_YEAR'],inplace=True)
    
    #fct_total_df.to_csv("total_sales.csv")
    
    fct_fmt_df = fmt_df[fmt_df['DATA_TYPE']=='FORECASTED']
    fct_fmt_df_vol = pd.pivot_table(fct_fmt_df,index=['MONTH_YEAR'],columns='FORMAT',values='VOLUME')
    fct_fmt_df_val = pd.pivot_table(fct_fmt_df,index=['MONTH_YEAR'],columns='FORMAT',values='VALUE')
    
    fct_fmt_df_vol['format_total'] = fct_fmt_df_vol.sum(numeric_only=True, axis=1)
    fct_fmt_df_val['format_total'] = fct_fmt_df_val.sum(numeric_only=True, axis=1)
    
    #fct_fmt_df_vol.to_csv("format_before_calib.csv")
    
    fmt_column_list = list(fct_fmt_df_vol.columns)
    for fmt_names in fmt_column_list:
        fct_fmt_df_vol[fmt_names] = fct_fmt_df_vol[fmt_names] *(fct_total_df['VOLUME']/fct_fmt_df_vol['format_total'])
        fct_fmt_df_val[fmt_names] = fct_fmt_df_val[fmt_names] *(fct_total_df['VALUE']/fct_fmt_df_val['format_total'])
    
    
    #fct_fmt_df_vol.to_csv("format_after_calib.csv")
    
    
    fct_fmt_df_vol.reset_index(inplace=True)
    fct_fmt_df_val.reset_index(inplace=True)
    
    
    fct_fmt_df_vol_up = pd.melt(fct_fmt_df_vol,id_vars=['MONTH_YEAR'],value_vars=fmt_column_list)
    fct_fmt_df_vol_up.rename(columns = {"value":"VOLUME"},inplace=True)
    
    fct_fmt_df_val_up = pd.melt(fct_fmt_df_val,id_vars=['MONTH_YEAR'],value_vars=fmt_column_list)
    fct_fmt_df_val_up.rename(columns = {"value":"VALUE"},inplace=True)
    
    final_df = fct_fmt_df_vol_up.merge(fct_fmt_df_val_up,on=['MONTH_YEAR','FORMAT'])
    final_df.dropna(subset=['VOLUME', 'VALUE'],inplace=True)
    final_df["DATA_TYPE"] = 'FORECASTED'
    
    return final_df



def ul_generic_channel_calibration(total_df,chnl_df,geo_id,catg_cd,sub_catg_cd):
    
    
    #total_sales_df['VOLUME'] = total_sales_df['VOLUME'].astype(float)
    #total_sales_df['VALUE'] = total_sales_df['VALUE'].astype(float)
    
    #fct_total_df = total_sales_df[total_sales_df['RECORD_TYPE']=='FORECAST']
    #fct_total_df['VOLUME'] = fct_total_df['VOLUME'] + (fct_total_df['VOLUME']*0.05)
    
    fct_total_df = total_df.copy()
    fct_total_df.set_index(['MONTH_YEAR'],inplace=True)
    
    #fct_total_df.to_csv("total_sales.csv")    
    
    fct_chnl_df = chnl_df[chnl_df['DATA_TYPE']=='FORECASTED']
    fct_chnl_df_vol = pd.pivot_table(fct_chnl_df,index=['MONTH_YEAR'],columns='CHANNEL',values='VOLUME')
    fct_chnl_df_val = pd.pivot_table(fct_chnl_df,index=['MONTH_YEAR'],columns='CHANNEL',values='VALUE')
    
    #fct_chnl_df_vol['channel_total'] = fct_chnl_df_vol.sum(numeric_only=True, axis=1)
    #fct_chnl_df_val['channel_total'] = fct_chnl_df_val.sum(numeric_only=True, axis=1)
    
    select_query3 = """select a.chnl_desc, b.chnl_desc,b.rollup_group  
                        from """+hana_db+""".channel_type a
                        join """+hana_db+""".channel_type b on b.rollup_chnl = a. chnl_cd and a.geo_id = b.geo_id
                        where a.geo_id = ?
                        and b.chnl_cd in 
                        (select distinct CHNL_CD  
                        	from """+hana_db+""".res_recordset_save a 
                        	join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ctry_geo_id = ? and ul_region_name = 'Country'
                        	where a.CATG_CD = ?
                        	AND a.SUBCAT_CD = ?
                        	AND a.SECTOR_SCENARIO_CD = 1) """
            
    input_values = (geo_id,geo_id,catg_cd,sub_catg_cd)
    
    #input_values = ("181","181","9","2")
    
    cursor_data = connection.cursor()
    cursor_data.execute(select_query3,input_values)
    rows = cursor_data.fetchall()
    column_names2 = ["chnl_desc","rollup_chnl","group"]
    rollup_channel_DF = pd.DataFrame(rows,columns=column_names2)
    #print(rollup_channel_DF)
    cursor_data.close()
    
    final_column_names = list(fct_chnl_df_vol.columns)
    
    if rollup_channel_DF.shape[0] >1:
        roll_up_channels = rollup_channel_DF['rollup_chnl'].to_list()
        
        column_list = list(fct_chnl_df_vol.columns)
        #print(column_list)
        temp_chnl = roll_up_channels.copy()
        
        for channel in temp_chnl:
            if channel in column_list:
                column_list.remove(channel)
            else:
                roll_up_channels.remove(channel)
        
        fct_chnl_df_vol['channel_total'] = fct_chnl_df_vol[column_list].sum(numeric_only=True, axis=1)
        fct_chnl_df_val['channel_total'] = fct_chnl_df_val[column_list].sum(numeric_only=True, axis=1)
        
        # testing
        #fct_chnl_df_vol.to_csv("channel_before_calib.csv")
        
        for chnl_names in column_list:
            fct_chnl_df_vol[chnl_names] = fct_chnl_df_vol[chnl_names] *(fct_total_df['VOLUME']/fct_chnl_df_vol['channel_total'])
            fct_chnl_df_val[chnl_names] = fct_chnl_df_val[chnl_names] *(fct_total_df['VALUE']/fct_chnl_df_val['channel_total'])
         
        grouping = list(rollup_channel_DF['group'].unique())      
        
        for group_set in grouping:
            tempDF = rollup_channel_DF[rollup_channel_DF['group']==group_set]
            main_channels = tempDF['chnl_desc'].to_list()
            rollup_channels = tempDF['rollup_chnl'].to_list()
            fct_chnl_df_vol['channel_total'] = fct_chnl_df_vol[rollup_channels].sum(numeric_only=True, axis=1)
            fct_chnl_df_val['channel_total'] = fct_chnl_df_val[rollup_channels].sum(numeric_only=True, axis=1)
            for i in range(0, len(rollup_channels)):
                fct_chnl_df_vol[rollup_channels[i]] = fct_chnl_df_vol[rollup_channels[i]] *(fct_chnl_df_vol[main_channels[i]]/fct_chnl_df_vol['channel_total'])
                fct_chnl_df_val[rollup_channels[i]] = fct_chnl_df_val[rollup_channels[i]] *(fct_chnl_df_val[main_channels[i]]/fct_chnl_df_val['channel_total'])
            
    else:
        
        column_list = list(fct_chnl_df_vol.columns)
        
        fct_chnl_df_vol['channel_total'] = fct_chnl_df_vol.sum(numeric_only=True, axis=1)
        fct_chnl_df_val['channel_total'] = fct_chnl_df_val.sum(numeric_only=True, axis=1)
        # testing
        
        #fct_chnl_df_vol.to_csv("channel_before_calib.csv")
        
        for chnl_names in column_list:
            fct_chnl_df_vol[chnl_names] = fct_chnl_df_vol[chnl_names] *(fct_total_df['VOLUME']/fct_chnl_df_vol['channel_total'])
            fct_chnl_df_val[chnl_names] = fct_chnl_df_val[chnl_names] *(fct_total_df['VALUE']/fct_chnl_df_val['channel_total'])
         
    
    #fct_chnl_df_vol.to_csv("channel_after_calib.csv")
    
    fct_chnl_df_vol.reset_index(inplace=True)
    fct_chnl_df_val.reset_index(inplace=True)
        
    
    fct_chnl_df_vol_up = pd.melt(fct_chnl_df_vol,id_vars=['MONTH_YEAR'],value_vars=final_column_names)
    fct_chnl_df_vol_up.rename(columns = {"value":"VOLUME"},inplace=True)
        
    fct_chnl_df_val_up = pd.melt(fct_chnl_df_val,id_vars=['MONTH_YEAR'],value_vars=final_column_names)
    fct_chnl_df_val_up.rename(columns = {"value":"VALUE"},inplace=True)
        
    final_df = fct_chnl_df_vol_up.merge(fct_chnl_df_val_up,on=['MONTH_YEAR','CHANNEL'])
    final_df.dropna(subset=['VOLUME', 'VALUE'],inplace=True)
    final_df["DATA_TYPE"] = 'FORECASTED'
    
    return final_df