# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 17:04:37 2021

@author: Rameshkumar
"""
import json


def ul_db_excel_summary_country_list(request_header):
    

    responseData ={"request_header":request_header,
               "Countries agreed": ["Indonesia", "United Kingdom", "South Africa","Germany","Netherlands","China","India", "France","Brazil","Mexico", "Philippines"],
               "Countries WIP":[]
               }
    
    json_object = json.dumps(responseData)
    return(json_object)


