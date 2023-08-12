#!/home/sanjay/anaconda3/bin/python3 -u
#!/cygdrive/c/ProgramData/Anaconda3/python -u

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
	
	select_query = """select ctry_name, div_desc, catg_desc, subcat_desc, chnl_desc, MONTH_YEAR, sum(SALES_VALUE) 
			from (select
					f.ctry_name,
					c.div_desc,
					e.catg_desc,
					b.subcat_desc,
					g.chnl_desc,
					CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
					SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].SALES_VOLUME') as decimal)* cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE 
				from """+hana_db+""".res_recordset_save a 
					join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ul_region_name = 'Country' 
					join """+hana_db+""".geo_hier f on z.ctry_geo_id = f.geo_id 
					join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd 
					join """+hana_db+""".division_type c on c.div_cd = e.div_cd 
					join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = z.ctry_geo_id and b.catg_cd = e.catg_cd 
					join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD 
					join """+hana_db+""".channel_type g on a.chnl_cd = g.chnl_cd and g.geo_id = z.ctry_geo_id
				where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'ACTUAL' 
				  and a.fmt_cd =0 
				  and a.SECTOR_SCENARIO_CD = 1 
				  and year(a.period_ending_date) in (%s, %s, %s, %s) 
				group by b.subcat_desc, e.catg_desc, f.ctry_name, c.div_desc, chnl_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) 
			union 
			select
				f.ctry_name,
				c.div_desc,
				e.catg_desc,
				b.subcat_desc,
				g.chnl_desc,
				CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) as MONTH_YEAR,
				SUM(cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PREDICTED_VOLUME') as decimal)* cast(JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].PRICE_PER_VOL') as decimal)) as SALES_VALUE 
			from """+hana_db+""".res_recordset_save a 
				join """+hana_db+""".ul_geo_hier z on a.ul_geo_id = z.ul_rgn_geo_id and z.ul_region_name = 'Country' 
				join """+hana_db+""".geo_hier f on z.ctry_geo_id = f.geo_id 
				join """+hana_db+""".category_type e on a.catg_cd = e.catg_cd 
				join """+hana_db+""".division_type c on c.div_cd = e.div_cd
				join """+hana_db+""".subcat_type b on a.subcat_cd = b.subcat_cd and b.geo_id = z.ctry_geo_id and b.catg_cd = e.catg_cd 
				join """+hana_db+""".sector_scenario_type d on a.SECTOR_SCENARIO_CD = d.SECTOR_SCENARIO_CD 
				join """+hana_db+""".channel_type g on a.chnl_cd = g.chnl_cd and g.geo_id = z.ctry_geo_id
			where JSON_VALUE(a.RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') = 'FORECAST' 
			  and a.fmt_cd =0 
			  and a.SECTOR_SCENARIO_CD = %s 
			  and year(a.period_ending_date) in (%s, %s, %s, %s) 
			group by b.subcat_desc, e.catg_desc, f.ctry_name, c.div_desc, chnl_desc, CONCAT(year(a.PERIOD_ENDING_DATE), '-Q', quarter(a.PERIOD_ENDING_DATE)) 
			      ) sub
			group by MONTH_YEAR, ctry_name, div_desc, catg_desc, subcat_desc, chnl_desc
			order by ctry_name, div_desc, subcat_desc, chnl_desc, MONTH_YEAR"""
	
   
	cursor_data = connection.cursor()
	cursor_data.execute(select_query,input_values)
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
	#print("col_names: ",col_names)
	#for i in range (4,len(col_names)):
	for i in range (5,len(col_names)):
		tempDF[col_names[i]] = round((tempDF[col_names[i]]/1000000)*exch_rate,2)

	return tempDF



#HERE
def ul_db_excel_summary_report(parameters):

	#For testing
	#parameters =  {"COUNTRY":"all"}
	
	# year config to generate excel summary report 
	pre_year1 = '2019'
	pre_year2 = '2020'
	curr_year = '2021'
	next_year = '2022'

	#country_list = ['Indonesia','United Kingdom','South Africa']
	
	
	column_names = ["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","CHANNEL","MONTH_YEAR","VALUE"]
	
	#print("\nget sales data for Baseline scenario...")
	# to get sales data for Baseline scenario
	input_values =(pre_year1,pre_year2,curr_year,next_year,"1",pre_year1,pre_year2,curr_year,next_year)
	baseline_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
	
	
	if baseline_DF.shape[0] <1:
		responseData ={"request_header":parameters,
					   "EXCEL_DATA":""
						 }
			
		json_object = json.dumps(responseData)
		return json_object

	
	#print("\nget sales data for Long Covid scenario ...")
	# to get sales data for Long Covid scenario 
	input_values =(pre_year1,pre_year2,curr_year,next_year,"4",pre_year1,pre_year2,curr_year,next_year)
	longcovid_DF = pd.DataFrame(get_global_sales_data(input_values),columns=column_names)
	
	#print("\nget sales data for Return of inflation scenario ...")
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
	
	#print("\nPivoting...")
	baselinep_DF = pd.pivot_table(baseline_DF,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","CHANNEL"],columns='MONTH_YEAR',values='VALUE')
	longcovidp_DF = pd.pivot_table(longcovid_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","CHANNEL"],columns='MONTH_YEAR',values='VALUE')
	roip_DF = pd.pivot_table(roi_DF_new,index=["COUNTRY","DIVISION","CATEGORY","SUB_CATEGORY","CHANNEL"],columns='MONTH_YEAR',values='VALUE')
	
	
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
	
	final_DF = pd.DataFrame()
	country_list = list(combined_DF['COUNTRY'].unique())

	#country_list = ['Indonesia','United Kingdom','South Africa']
	#country_list = ['Brazil']
	#country = 'India'

	#print("\nEntering Main loop")
	for country in country_list:
		#print("\nCountry:",country)
		tempDF = combined_DF[combined_DF['COUNTRY']==country].copy()
		tempDF.fillna(0,inplace =True)
		#tempDF = currenty_conversion(tempDF,country)
		if tempDF.shape[0] > 1:
		#{
			div_list = list(tempDF['DIVISION'].unique())
			#div_name ='BPC'
			for div_name in div_list:
			#{
				#print("\tDivision:",div_name)
				tempDF2 = tempDF[tempDF['DIVISION']==div_name].copy()
				if tempDF2. shape[0] >1:
				#{
					category_list = list(tempDF2['CATEGORY'].unique())
					#catg_name = 'Hair Care'
					for catg_name in category_list:
						#print("\t\tCategory:",catg_name)
						tempDF3 = tempDF2[tempDF2['CATEGORY']==catg_name].copy()
						if tempDF3.shape[0] >1:
						#{
							subcat_list = list(tempDF3['SUB_CATEGORY'].unique())
							subcat_knt = len(subcat_list)
							#print("subcat_knt:",subcat_knt)
							tempDF9 = pd.DataFrame()
							for subcat_name in subcat_list:
								#print("\t\t\tSubCat:",subcat_name)
								tempDF4 = tempDF3[tempDF3['SUB_CATEGORY']==subcat_name].copy()

								#Accumulate SubCat total if >1 subcats
								if subcat_knt > 1:
									if tempDF9.shape[0] < 1:
										tempDF9 = tempDF4.copy()
									else:
										tempDF9 = tempDF9.append(tempDF4)

								if tempDF4.shape[0] >1:
								#{
									df_len = max(tempDF4.index)+1
									tempDF4.loc[df_len] = tempDF4.sum(numeric_only=True, axis=0) 
									tempDF4.at[df_len, 'COUNTRY'] = country
									tempDF4.at[df_len, 'DIVISION'] = div_name
									tempDF4.at[df_len, 'CATEGORY'] = catg_name
									tempDF4.at[df_len, 'SUB_CATEGORY'] = subcat_name
									tempDF4.at[df_len, 'CHANNEL'] = "Total"

									if final_DF.shape[0] < 1:
										final_DF = tempDF4.copy()
									else:
										final_DF = final_DF.append(tempDF4)
								#}
								else:
									if final_DF.shape[0] < 1:
										final_DF = tempDF4.copy()
									else:
										final_DF = final_DF.append(tempDF4)
							#Print Subcat totals if more than one subcat
							if subcat_knt > 1:
							#{
								#print("\t\tadding Subcat totals")

								df_len = max(tempDF9.index)+1
								tempDF9.loc[df_len] = tempDF9.sum(numeric_only=True, axis=0) 
								tempDF9.at[df_len, 'COUNTRY'] = country
								tempDF9.at[df_len, 'DIVISION'] = div_name
								tempDF9.at[df_len, 'CATEGORY'] = catg_name
								tempDF9.at[df_len, 'SUB_CATEGORY'] = "Total"
								tempDF9.at[df_len, 'CHANNEL'] = "Total"

								tempDF10 = tempDF9[tempDF9['SUB_CATEGORY'] == 'Total']

								if final_DF.shape[0] < 1:
									final_DF = tempDF10.copy()
								else:
									final_DF = final_DF.append(tempDF10)
							#}
						#}
						else:
							if final_DF.shape[0] < 1:
								final_DF = tempDF3.copy()
							else:
								final_DF = final_DF.append(tempDF3)
				#}
				else:
					if final_DF.shape[0] < 1:
						final_DF = tempDF2.copy()
					else:
						final_DF = final_DF.append(tempDF2)
			#}

			#print("\n\tEntering div_list loop")
			for div_name in div_list:
				#print("\tDivision:",div_name)
				tempDF5 = tempDF[tempDF['DIVISION']==div_name].copy()
				if tempDF5.shape[0] >1:
					df_len = max(final_DF.index)+1
					#print("\t\tdf_len:",df_len)
					final_DF.loc[df_len] = tempDF5.sum(numeric_only=True, axis=0) 
					final_DF.at[df_len, 'COUNTRY'] = country
					final_DF.at[df_len, 'DIVISION'] = div_name
					final_DF.at[df_len, 'CATEGORY'] = "Total"
					final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"
					final_DF.at[df_len, 'CHANNEL'] = "Total"
				
				else:
					df_len = max(tempDF5.index)
					tempDF5.at[df_len, 'COUNTRY'] = country
					tempDF5.at[df_len, 'DIVISION'] = div_name
					tempDF5.at[df_len, 'CATEGORY'] = "Total"
					tempDF5.at[df_len, 'SUB_CATEGORY'] = "Total"	   
					tempDF5.at[df_len, 'CHANNEL'] = "Total"	   
				   
					if final_DF.shape[0] < 1:
						final_DF = tempDF5.copy()
					else:
						final_DF = final_DF.append(tempDF5)
			#print("\n\tExiting div_list loop")
		
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
			final_DF.at[df_len, 'CHANNEL'] = "Total"
		
		
		if tempDF.shape[0] >=1:
			df_len = max(final_DF.index)+1
			final_DF.loc[df_len] = tempDF.sum(numeric_only=True, axis=0) 
			final_DF.at[df_len, 'COUNTRY'] = country
			final_DF.at[df_len, 'DIVISION'] = "Total"
			final_DF.at[df_len, 'CATEGORY'] = "Total"
			final_DF.at[df_len, 'SUB_CATEGORY'] = "Total"		
			final_DF.at[df_len, 'CHANNEL'] = "Total"		
				   
	
	
	#print("\nConverting to EURO...")
	# convery to EURO based on country exchange rate
	final_DF2 = pd.DataFrame()
	for country in country_list:
		#print("\tCountry:",country)
		tempDF = final_DF[final_DF['COUNTRY']==country].copy()
		tempDF = currenty_conversion(tempDF,country)
		if final_DF2.shape[0] < 1:
			final_DF2 = tempDF.copy()
		else:
			final_DF2 = final_DF2.append(tempDF,ignore_index=True)
	
  
	#print("\nCalculating global total values...")
	# calculate global total values  
	tempDF6 = final_DF2[(final_DF2['CHANNEL']=='Total') &(final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='BPC') ].copy()   
	df_len = max(final_DF2.index)+1
	final_DF2.loc[df_len] = tempDF6.sum(numeric_only=True, axis=0) 
	final_DF2.at[df_len, 'COUNTRY'] = "Global"
	final_DF2.at[df_len, 'DIVISION'] = "BPC"
	final_DF2.at[df_len, 'CATEGORY'] = "Total"
	final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
	final_DF2.at[df_len, 'CHANNEL'] = "Total"
	
	tempDF7 = final_DF2[(final_DF2['CHANNEL']=='Total') & (final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='F&R') ].copy()   
	df_len = max(final_DF2.index)+1
	final_DF2.loc[df_len] = tempDF7.sum(numeric_only=True, axis=0) 
	final_DF2.at[df_len, 'COUNTRY'] = "Global"
	final_DF2.at[df_len, 'DIVISION'] = "F&R"
	final_DF2.at[df_len, 'CATEGORY'] = "Total"
	final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
	final_DF2.at[df_len, 'CHANNEL'] = "Total"
	
	tempDF8 = final_DF2[(final_DF2['CHANNEL']=='Total') & (final_DF2['CATEGORY']=='Total') & (final_DF2['SUB_CATEGORY']=='Total') & (final_DF2['DIVISION']=='HC') ].copy()   
	df_len = max(final_DF2.index)+1
	final_DF2.loc[df_len] = tempDF8.sum(numeric_only=True, axis=0) 
	final_DF2.at[df_len, 'COUNTRY'] = "Global"
	final_DF2.at[df_len, 'DIVISION'] = "HC"
	final_DF2.at[df_len, 'CATEGORY'] = "Total"
	final_DF2.at[df_len, 'SUB_CATEGORY'] = "Total"
	final_DF2.at[df_len, 'CHANNEL'] = "Total"
	
	rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='BPC'),:]
	final_DF = final_DF.append(rows, ignore_index=True)
	
	rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='F&R'),:]
	final_DF = final_DF.append(rows, ignore_index=True)
	
	rows = final_DF2.loc[(final_DF2['COUNTRY']=='Global') &(final_DF2['DIVISION']=='HC'),:]
	final_DF = final_DF.append(rows, ignore_index=True)
	
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
	
	
	
	#print("\nCapturing baseline scenario growth numbers ...")
	# capture baseline scenario growth numbers 
	final_DF2["BS_H1_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]))-1)*100
	final_DF2["BS_H2_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
	final_DF2["BS_FY_"+curr_year +"_"+pre_year2] = (((final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"]+final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
	final_DF2["BS_FY_"+ next_year +"_"+pre_year1] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
	final_DF2["BS_FY_"+ next_year +"_"+curr_year] = (((final_DF["BS_"+ next_year +"_Q1"] + final_DF["BS_"+ next_year +"_Q2"]+final_DF["BS_"+ next_year +"_Q3"] + final_DF["BS_"+ next_year +"_Q4"])/(final_DF["BS_"+curr_year +"_Q1"] + final_DF["BS_"+curr_year +"_Q2"] + final_DF["BS_"+curr_year +"_Q3"] + final_DF["BS_"+curr_year +"_Q4"]))-1)*100
	
	
	#print("\nCapturing long covid growth numbers ...")
	# capture long covid growth numbers 
	final_DF2["LC_H1_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]))-1)*100
	final_DF2["LC_H2_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
	final_DF2["LC_FY_"+curr_year +"_"+pre_year2] = (((final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"]+final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
	final_DF2["LC_FY_"+ next_year +"_"+pre_year1] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
	final_DF2["LC_FY_"+ next_year +"_"+curr_year] = (((final_DF["LC_"+ next_year +"_Q1"] + final_DF["LC_"+ next_year +"_Q2"]+final_DF["LC_"+ next_year +"_Q3"] + final_DF["LC_"+ next_year +"_Q4"])/(final_DF["LC_"+curr_year +"_Q1"] + final_DF["LC_"+curr_year +"_Q2"] + final_DF["LC_"+curr_year +"_Q3"] + final_DF["LC_"+curr_year +"_Q4"]))-1)*100
	
	#print("\nCapturing ROI scenario growth numbers ...")
	# capture ROI scenario growth numbers 
	final_DF2["RI_H1_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]))-1)*100
	final_DF2["RI_H2_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
	final_DF2["RI_FY_"+curr_year +"_"+pre_year2] = (((final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"]+final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"])/(final_DF["A_"+pre_year2 +"_Q1"] + final_DF["A_"+pre_year2 +"_Q2"] + final_DF["A_"+pre_year2 +"_Q3"] + final_DF["A_"+pre_year2 +"_Q4"]))-1)*100
	final_DF2["RI_FY_"+ next_year +"_"+pre_year1] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["A_"+pre_year1+"_Q1"] + final_DF["A_"+pre_year1+"_Q2"] + final_DF["A_"+pre_year1+"_Q3"] + final_DF["A_"+pre_year1+"_Q4"]))-1)*100
	final_DF2["RI_FY_"+ next_year +"_"+curr_year] = (((final_DF["RI_"+ next_year +"_Q1"] + final_DF["RI_"+ next_year +"_Q2"]+final_DF["RI_"+ next_year +"_Q3"] + final_DF["RI_"+ next_year +"_Q4"])/(final_DF["RI_"+curr_year +"_Q1"] + final_DF["RI_"+curr_year +"_Q2"] + final_DF["RI_"+curr_year +"_Q3"] + final_DF["RI_"+curr_year +"_Q4"]))-1)*100
	
	final_DF2.replace({-100:0},inplace=True)
	#print("\nfinal_DF2 saved to: summary_channel.csv")
	#final_DF2.to_csv("summary_channel.csv")
	
	colnames = final_DF2.columns
	final_DF2[colnames[4:]] = final_DF2[colnames[4:]].round(2)
	
	responseDict2 = final_DF2.to_dict(orient='records')

	responseData ={"request_header":parameters,
				   "EXCEL_DATA":responseDict2
					 }
		
	json_object = json.dumps(responseData)
	return json_object


########################################################################
# unit testing
#input_vaiables= {"COUNTRY":"all"}
# output1 = ul_db_excel_summary_report(input_vaiables)
#print(output1)
	
