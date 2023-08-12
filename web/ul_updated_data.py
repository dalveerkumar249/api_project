# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 10:22:48 2021

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
    

def ul_updated_data(parameters):
   
    select_query = """select c.subcat_desc,e.ctry_name,b.catg_desc,JSON_VALUE(RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE') as record_type,
                    SUBSTRING(MONTHNAME(min(period_ending_date)),0,3) ||'_'|| YEAR(min(period_ending_date)) || ' - ' ||
                    SUBSTRING(MONTHNAME(max(period_ending_date)),0,3) ||'_'|| YEAR(max(period_ending_date)) as period_ending_date, 
                    max(pred_updated_date)
                    from """+hana_db+""".res_recordset_save a
                    join """+hana_db+""".category_type b on a.catg_cd = b.catg_cd
                    join """+hana_db+""".ul_geo_hier d on  d.ul_rgn_geo_id = a.ul_geo_id and d.ul_region_name = 'Country'
                    join """+hana_db+""".subcat_type c  on a.subcat_cd = c.subcat_cd and c.catg_cd = b.catg_cd and c.geo_id = d.ctry_geo_id
                    join """+hana_db+""".geo_hier e on d.ctry_geo_id = e.geo_id and e.region_name = 'DUMMY'
                    where 
                    a.SECTOR_SCENARIO_CD = 1
                    group by c.subcat_desc,e.ctry_name,b.catg_desc,JSON_VALUE(RECORDSET_CONTENTS_JSON, '$[*].RECORD_TYPE')
                    order by e.ctry_name,c.subcat_desc,b.catg_desc"""

    cursor_data = connection.cursor()
    cursor_data.execute(select_query)
    rows = cursor_data.fetchall()
    #rows = cursor_data.fetchmany(5)
    #return(str(rows))

    
    column_names = ["SUB CATEGORY","COUNTRY","CATEGORY","RECORD_TYPE","PRED_ENDING_DATE","LAST UPDATED"]
    tempDF = pd.DataFrame(rows,columns=column_names)
    #close DB connection
    cursor_data.close()
    
    actualDF = tempDF[tempDF['RECORD_TYPE'] =='ACTUAL']
    predictedDF = tempDF[tempDF['RECORD_TYPE'] =='FORECAST']
    #print(str(tempDF))
    
    actualDF.columns = ["SUB CATEGORY","COUNTRY","CATEGORY","RECORD_TYPE","ACTUAL","LAST UPDATED"]
    predictedDF.columns = ["SUB CATEGORY","COUNTRY","CATEGORY","RECORD_TYPE","FORECAST","LAST UPDATED"]
    
    predictedDF = predictedDF[["SUB CATEGORY","COUNTRY","CATEGORY","FORECAST"]]
    resultsDF = pd.merge(actualDF,predictedDF,how='left',left_on=["SUB CATEGORY","COUNTRY","CATEGORY"],
                         right_on = ["SUB CATEGORY","COUNTRY","CATEGORY"])
    
    resultsDF.drop(['RECORD_TYPE'],axis=1,inplace=True)
    resultsDF.insert(3,'DATA FORMAT','Monthly')
    resultsDF.rename(columns={"SUB CATEGORY":"CELL NAME"},inplace=True)
    resultsDF["LAST UPDATED"] = resultsDF["LAST UPDATED"].apply(lambda x: x.strftime('%b %d, %Y'))
    
    resultsDF = resultsDF[["CELL NAME","COUNTRY","CATEGORY","DATA FORMAT","ACTUAL","FORECAST","LAST UPDATED"]]
    #resultsDF.to_csv("test.csv")
    responseDict = resultsDF.to_dict(orient='records')
    
    responseData ={"request_header":parameters,
                   "data": responseDict}
    
    json_object = json.dumps(responseData)  
    return json_object 

#input_data = {"DUMMY":1}
#temp = ul_updated_data(input_data)
#print(temp)