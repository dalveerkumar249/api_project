###########################################################################################
# This program is the Main demo API program for the frontend
###########################################################################################
# 26 Mar 2021 - Sanjay - Originally coded
# 11 Mar 2022 - Ramesh - updated this program for catman demo front end
###########################################################################################
#Listening port: 4860
#
#
#
#
#
#
#
###########################################################################################

from flask import Flask, request
import json

#import io
#import csv

# new api calls for UL prod
import ul_forecast
import ul_updated_data
import ul_treemap
import ul_growth_rate
import ul_input_contribution_v1
import ul_input_validation
import ul_simulation_new_v2
import ul_simulation_new_v3


import ul_ppt_frontend
import ul_excel_summary_report_V1
import ul_excel_summary_report_V2
import ul_excel_summary_report_volume


import ul_excel_summary_channel_V1
import ul_excel_summary_channel_V4
import ul_excel_summary_country_list

import ul_driver_decomposition

#from flask_mysqldb import MySQL
import MySQLdb.cursors
import pymysql 



application = Flask(__name__)
application.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024

#####################################################
# NEW API Calls UL production implementation
#####################################################
    

@application.route('/forecast_vol', methods=['POST']) #----------------1 ok
def func_forecast_vol():
    parameters = request.json
    print(parameters)
    #return "Hello"

    #These are values from frontend dropdowns and switches
    geo_id = parameters['COUNTRY_NAME'] # 73
    catg_cd = parameters['CATAGORY_NAME'] #1
    sub_catg_cd = parameters['SUB_CATEGORY_NAME'] #3
    metric = parameters['METRIC'] #"avg"
    division = parameters['DIVISION']

    # json_string = dapi_01.cat_forecast_vol(parameters)
    json_string = ul_forecast.ul_db_forecast_vol(geo_id,division,catg_cd,sub_catg_cd,metric)
    
    #response = jsonify(json_string)
    #response.headers.add("Access-Control-Allow-Origin", "*")
    return json_string

   

@application.route('/forecast_val', methods=['POST']) #----------------2 ok
def func_forecast_val():
    parameters = request.json
    print(parameters)
    #These are values from frontend dropdowns and switches
    json_string = ul_forecast.ul_db_forecast_val(parameters)
    # json_string = dapi_02.cat_forecast_val(parameters)
    # response = jsonify(json_string)
    # response.headers.add("Access-Control-Allow-Origin", "*")
    return json_string


@application.route('/growth_per_volume', methods=['POST'])  #----------------3 ok
def func_growth_per_volume():
    
    print("inside func_growth_per_volume")
    try:
        parameters = request.json
        #json_string = dapi_03.cat_growth_per_volume(content)
        geo_id = parameters['COUNTRY_NAME']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        year = parameters['YEAR']
        comp_year =  parameters['COMPARISON_YEAR']
        forecast_secenario = parameters['FORECAST_SCENARIO']
        division = parameters['DIVISION']
        metric = parameters['METRIC']
        #return(parameters['CATAGORY_NAME'])
        
        json_string = ul_growth_rate.ul_db_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd, year,comp_year, forecast_secenario, metric)
        
        response = json_string
        return response
    except:
        #raise
        print ("EXC")
        print("Incorrect parameters",request.json)
        responseData ={"request_header":request.json,
               "sales_volume": []}
        json_object = json.dumps(responseData)  
         #print(json_object)
        return json_object
     
@application.route('/update_data', methods=['POST']) #-------------4 ok
def func_update_data():
    print("inside Month year data")
    content = request.json
    json_string = ul_updated_data.ul_updated_data(content)
    # json_string = dapi_04.cat_data(content)
    response = json_string

    return response

@application.route('/growth_per_value', methods=['POST']) #-----------5 ok
def func_growth_per_value():
    print("inside func_growth_per_value")
    content = request.json
    print(content)    
    #json_string = dapi_05.cat_growth_per_value(content)
    json_string = ul_growth_rate.ul_db_growth_rate_val(content)
    response = json_string

    return response

@application.route('/input_contribtion', methods=['POST']) #-------------6 ok
def func_input_contribtion():
    print("inside func_input_contribtion")
    #content = request.json
    #json_string = dapi_06A.cat_input_contribtion(content)
    print(request.json)
    try:
        parameters = request.json
        geo_id = parameters['COUNTRY_NAME']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_secenario = parameters['FORECAST_SCENARIO']
        division = parameters['DIVISION']
        metric = parameters['METRIC']
        json_string = ul_input_contribution_v1.ul_db_input_contribution(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"table")
        

        response = json_string
        return response
    except:
        raise
        print("incorrect input variables",request.json)
        
@application.route('/qtr_input_contribtion', methods=['POST']) #----------7 ok
def func_qtr_input_contribtion():
    
    print("inside Month year qtr_input_contribtion")
    # content = request.json
    # json_string = dapi_07A.cat_qtr_input_contribution(content)
   
    try:
        parameters = request.json
        geo_id = parameters['COUNTRY_NAME']
        catg_cd = parameters['CATAGORY_NAME']
        sub_catg_cd = parameters['SUB_CATEGORY_NAME']
        forecast_secenario = parameters['FORECAST_SCENARIO']
        division = parameters['DIVISION']
        #forecast_secenario = 1
        metric = parameters['METRIC']
        json_string = ul_input_contribution_v1.ul_db_input_contribution(geo_id,division,catg_cd,sub_catg_cd, forecast_secenario, metric,"graph")

        response = json_string
        return response
    except:        
        print("incorrect input variables",request.json)

@application.route('/tree_map', methods=['POST']) #-------------8 ok
def func_tree_map():
    print("inside Month year tree_map")
    # content = request.json
    # json_string = dapi_08A.cat_tree_map(content)
   
    parameters = request.json
   
    #These are values from frontend dropdowns and switches
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']

    json_string = ul_treemap.ul_db_treemap(geo_id,division,catg_cd,sub_catg_cd,metric)
    

    return json_string

@application.route('/channel_wise_sales', methods=['POST']) #---------9 ok
def func_channel_wise_sales():
    
    print("inside Month year channel_wise_sales")
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']
    
    # json_string = dapi_06B.cat_channel_wise_sales(content)
    json_string = ul_forecast.ul_db_channel_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric)
    response = json_string

    return response

@application.route('/growth_rate_by_channel', methods=['POST']) #---------10 ok
def func_growth_rate_by_channel():
    print("inside Month year growth_rate_by_channel")
    #content = request.json
    #json_string = dapi_07B.cat_growth_rate_by_channel(content)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"growth")
        
    response = json_string

    return response

@application.route('/qtr_channel_growth', methods=['POST']) #-------11 ok
def func_qtr_channel_growth():
    print("inside Month year qtr_channel_growth")
    # content = request.json
    # json_string = dapi_08B.cat_qtr_channel_growth(content)
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    #forecast_secenario = 1
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"graph")
        
    response = json_string

    return response


@application.route('/individual_channel_contribution', methods=['POST']) #--------12 ok
def func_individual_channel_contribution():
    print("inside Month year individual_Channel_contribution")
    # content = request.json
    # json_string = dapi_09B.cat_individual_Channel_contribution(content)
   
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    forecast_secenario = 1
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_channel_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"ind")
        
    response = json_string

    return response

@application.route('/channel_tree_map', methods=['POST']) #-------13 ok
def func_channel_tree_map():
    print("inside Month year channel_tree_map")
    #content = request.json
    #json_string = dapi_10B.cat_channel_tree_map(content)
   
    parameters = request.json
   
    #These are values from frontend dropdowns and switches
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']

    json_string = ul_treemap.ul_db_treemap(geo_id,division,catg_cd,sub_catg_cd,metric)


    return json_string
# ##############################################################################
@application.route('/region_forecast_vol', methods=['POST']) #--------14 ok 
def func_region_forecast_vol():
    print("inside Month year region_forecast_vol")
    print(request.json)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']
    
    #json_string = dapi_06C.cat_region_forecast_vol(content)
    json_string = ul_forecast.ul_db_region_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric)
    response = json_string

    return response

@application.route('/growth_per_region', methods=['POST']) #---------15 ok
def func_growth_per_region():
    print("inside Month year growth_per_region")
    print(request.json)
    #content = request.json
    #json_string = dapi_07C.cat_growth_per_region(content)
   
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"growth")
        
    response = json_string

    return response


@application.route('/individual_region_contribtion', methods=['POST']) #---------16 ok
def func_individual_region_contribtion():
    print("inside Month year individual_region_contribtion")
    print(request.json)
    #content = request.json
    #json_string = dapi_08C.cat_individual_region_contribtion(content)
   
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"ind")
        
    response = json_string

    return response

@application.route('/qtr_region_growth', methods=['POST']) #---------17 ok
def func_qtr_region_growth():
    print("inside Month year qtr_region_growth")

    #content = request.json
    #json_string = dapi_09C.cat_qtr_region_growth(content)
    print(request.json)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    #forecast_secenario = 1
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_region_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"graph")
    response = json_string
    return response


@application.route('/format_forecast_vol', methods=['POST']) #----------- 18 ok
def func_format_forecast_vol():
    print("inside Month year region_forecast_vol")
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']
    
    #json_string = dapi_06C.cat_region_forecast_vol(content)
    json_string = ul_forecast.ul_db_format_wise_sales(geo_id,division,catg_cd,sub_catg_cd,metric)
    response = json_string

    return response

# ################################################################


@application.route('/growth_rate_by_formats', methods=['POST']) #-----------19 ok
def func_growth_per_formats():
    print("inside Month year growth_per_formats")
    print(request.json)
    #content = request.json
    #json_string = dapi_07D.cat_growth_rate_by_formats(content)
   
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    metric = parameters['METRIC']
    
    json_string = ul_growth_rate.ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"growth")
        
    response = json_string


    return response


@application.route('/qtr_format_growth', methods=['POST'])  #-----------20 ok
def func_qtr_growth_per_formats():
    print("inside Qtr growth_per_region")
    #content = request.json
    #json_string = dapi_08D.cat_qtr_format_growth(content)
   
    print(request.json)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    #forecast_secenario = 1
    metric = parameters['METRIC']
    json_string = ul_growth_rate.ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"graph")
  
    response = json_string
    return response


@application.route('/individual_format_contribution', methods=['POST']) #---------21 ok
def func_individual_format_contribution():
    print("inside individual_format_contribution")
    #content = request.json
    #json_string = dapi_09D.cat_individual_format_contribution(content)
   
    print(request.json)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    #forecast_secenario = 1
    metric = parameters['METRIC']
    json_string = ul_growth_rate.ul_db_format_growth_rate_vol(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario, metric,"ind")
  
    response = json_string
    return response


@application.route('/input_validation', methods=['POST']) #--------- 22 ok
def func_input_validation():
    print("inside individual_format_contribution")
    print(request.json)
    content = request.json
    json_string = ul_input_validation.ul_db_input_validation(content)
   
    response = json_string

    return response


@application.route('/input_validation_dd_list', methods=['POST'])  #-------- 23 ok
def func_input_validation_dd_list():
    print("inside func_input_validation_dd_list")
    print(request.json)
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    metric = parameters['METRIC']
    
    json_string = ul_input_validation.ul_db_input_validation_dropdown(geo_id,division,catg_cd,sub_catg_cd,metric)
   
    response = json_string

    return response

# ################################## simulation API calls ###########################################

@application.route('/simulation_hist_pred_data', methods=['POST']) #----------- 24 ok
def func_simulation_hist_pred_data():
    print("inside func_simulation_hist_pred_data")
    print(request.json)
    content = request.json
    #json_string = dapi_11.ul_db_simulation_hist_pred_data(content)
    json_string = ul_simulation_new_v2.ul_db_simulation_hist_pred_data(content)
   
    response = json_string

    return response


@application.route('/v1/simulation_hist_pred_data', methods=['POST']) #--------- 25 ok
def func_simulation_hist_pred_data_new():
    print("inside func_simulation_hist_pred_data")
    print(request.json)
    content = request.json
    #json_string = dapi_11.ul_db_simulation_hist_pred_data(content)
    json_string = ul_simulation_new_v3.ul_db_simulation_hist_pred_data(content)
   
    response = json_string

    return response



@application.route('/simulation_pred_sales_data', methods=['POST']) #---------- 26 X
def func_simulation_pred_sales_data():
    print("inside func_simulation_pred_sales_data")
    #print(request.json)
    content = request.json
    json_string = ul_simulation_new_v2.ul_db_simulation_pred_sales_data(content)
   
    response = json_string

    return response

@application.route('/v1/simulation_pred_sales_data', methods=['POST']) #------------- 27 X
def func_simulation_pred_sales_data_new():
    print("inside func_simulation_pred_sales_data")
    #print(request.json)
    content = request.json
    json_string = ul_simulation_new_v3.ul_db_simulation_pred_sales_data(content)
   
    response = json_string

    return response
    

@application.route('/simulation_hist_data_compare_scenarios', methods=['POST']) #------ 28 ok
def func_simulation_hist_data_compare_scenarios():
    print("inside func_simulation_pred_sales_data")
    #print(request.json)
    content = request.json
    json_string = ul_simulation_new_v2.ul_db_simulation_hist_data_compare_scenarios(content)
   
    response = json_string

    return response


@application.route('/v1/simulation_hist_data_compare_scenarios', methods=['POST']) #----------- 29 ok
def func_simulation_hist_data_compare_scenarios_new():
    print("inside func_simulation_pred_sales_data")
    #print(request.json)
    content = request.json
    json_string = ul_simulation_new_v3.ul_db_simulation_hist_data_compare_scenarios(content)
   
    response = json_string

    return response



# ################################## PPT API calls ###########################################


@application.route('/ppt_estimates_by_divisions', methods=['POST']) #------------------- 30 ok
def func_ppt_estimates_by_divisions():
       
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
   
    resultDF = ul_ppt_frontend.ul_db_estimates_by_Divisions(geo_id,division,catg_cd,sub_catg_cd,1)
    #return resultDF
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response



@application.route('/ppt_estimates_by_categories', methods=['POST']) #------------------- 31 ok
def func_estimates_by_categories():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_estimates_by_categories(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_retail_covid', methods=['POST']) #------------------- 32 ok
def func_ppt_retail_covid():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_retail_covid(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_channels_preference', methods=['POST']) #------------------- 33 ok
def func_ppt_channel_preference():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_channel_preference(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response



@application.route('/ppt_formats_preference', methods=['POST']) #------------------- 34 ok
def func_ppt_format_preference():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_format_preference(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_cpi_diff_scenarios', methods=['POST']) #------------------- 35 ok
def func_ppt_cpi_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"CPI")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_gdp_diff_scenarios', methods=['POST']) #------------------- 36 ok
def func_ppt_gdp_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"GDP")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_pdi_diff_scenarios', methods=['POST']) #------------------- 37 ok
def func_ppt_pdi_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"PDI")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_unemp_rate_diff_scenarios', methods=['POST']) #------------------- 38 ok
def func_ppt_unemp_rate_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"UNEMP_RATE")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response



@application.route('/ppt_covid_new_diff_scenarios', methods=['POST']) #------------------- 39 ok 
def func_ppt_covid_new_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"covid_new")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_covid_death_diff_scenarios', methods=['POST']) #------------------- 40 ok
def func_ppt_covid_death_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"covid_death")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_randrprctchange_diff_scenarios', methods=['POST'])  #------------ 41 ok
def func_randrprctchange_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"RARPC")
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_res_prctchange_diff_scenarios', methods=['POST']) #----------- 42 ok
def func_res_prctchange_diff_scenarios():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_macro_covid_diff_scenarios(geo_id,division,catg_cd,sub_catg_cd,1,"RPC")
    
    if resultDF.shape[0] < 1:
        responseData ={"request_header":parameters,
                   "ppt_data": []}
    else:
        responseDict = resultDF.to_dict(orient='list')
        responseDict = responseDict
        responseData ={"request_header":parameters,
                       "ppt_data": responseDict}
                
    json_object = json.dumps(responseData)
    
    response = json_object

    return response



@application.route('/ppt_forecast_vol', methods=['POST']) #----------- 43 ok
def func_ppt_forecast_vol():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_forecast_vol(geo_id,division,catg_cd,sub_catg_cd)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_vol_val_growth_rate_comparison', methods=['POST']) #----------- 44 ok
def func_ppt_vol_val_growth_rate_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    
    resultDF = ul_ppt_frontend.ul_db_ppt_vol_val_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/v1/ppt_vol_val_growth_rate_comparison', methods=['POST']) #----------- 45 ok
def func_ppt_vol_val_growth_rate_comparison_new():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    vol_metric = int(parameters['VOLUME_METRIC'])
    resultDF = ul_ppt_frontend.ul_db_ppt_vol_val_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,vol_metric)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

#==============================================================================================

@application.route('/ppt_val_vol_pct_change_year', methods=['POST']) ##------ 46 ok
def func_ppt_val_vol_pct_change_year():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_val_vol_pct_change_year(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response



@application.route('/ppt_input_contribution', methods=['POST']) #-------------47  get_data function not found
def func_ppt_input_contribution():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_input_contribution(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_contrib_macroeconomic', methods=['POST']) #-------------48 ok
def func_ppt_contrib_macroeconomic():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_contrib_macroeconomic(geo_id,division,catg_cd,sub_catg_cd,1)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_market_mix', methods=['POST']) ##-----49 ok
def func_ppt_market_mix():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    forecast_secenario = parameters['FORECAST_SCENARIO']
    #forecast_secenario = 1   
    resultDF = ul_ppt_frontend.ul_db_ppt_market_mix(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_channels_pect_contribution', methods=['POST']) #-------------50 ok
def func_ppt_channel_pct_contrib_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
   
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_channel_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_channel_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
       
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_channels_pect_growth', methods=['POST']) #-------------51 ok
def func_ppt_channel_pct_growth_rate_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
    
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_channel_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_channel_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
        
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_formats_pect_contribution', methods=['POST']) #-------------52 ok
def func_ppt_format_pct_contrib_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
    
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_format_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_format_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_formats_pect_growth', methods=['POST']) #-------------53 ok
def func_ppt_format_pct_growth_rate_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
    
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_format_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_format_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
   
    
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_regions_pect_contribution', methods=['POST']) #-------------54 ok
def func_ppt_region_pct_contrib_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
   
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_region_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_region_pct_contrib_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_regions_pect_growth', methods=['POST']) #-------------55 ok
def func_ppt_region_pct_growth_rate_comparison():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)
        
    if int(scenario_check)==0:
        resultDF = ul_ppt_frontend.ul_db_ppt_region_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,1)
    else:
        resultDF = ul_ppt_frontend.ul_db_ppt_region_pct_growth_rate_comparison(geo_id,division,catg_cd,sub_catg_cd,5)
   
    responseDict = resultDF.to_dict(orient='split')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response

@application.route('/ppt_sub_category_list', methods=['POST']) #-------------56 ok
def func_ppt_sub_category_list():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_sub_category_list(geo_id)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_actual_data_source', methods=['POST']) ##------ 57 ok
def func_ppt_actual_data_source():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    resultDF = ul_ppt_frontend.ul_db_ppt_actual_data_source(geo_id,division,catg_cd,sub_catg_cd)
   
    responseDict = resultDF.to_dict(orient='list')
    responseDict = responseDict
    responseData ={"request_header":parameters,
                   "ppt_data": responseDict}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_metric_name', methods=['POST']) ##------ 58 ok
def func_ppt_metric_name():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    metric_name = ul_ppt_frontend.ul_db_ppt_metric_name(geo_id,division,catg_cd,sub_catg_cd)

    responseData ={"request_header":parameters,
                   "metric":"Volume (in "+ str(metric_name)+")"}
            
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/ppt_granular_scenario_name', methods=['POST']) ##------ 59 ok
def func_ppt_granular_scenario_name():
    
    parameters = request.json
    geo_id = parameters['COUNTRY_NAME']
    division = parameters['DIVISION']
    catg_cd = parameters['CATAGORY_NAME']
    sub_catg_cd = parameters['SUB_CATEGORY_NAME']
    #forecast_secenario = parameters['FORECAST_SCENARIO']
       
    scenario_check = ul_ppt_frontend.ul_db_ppt_price_change_check(geo_id,catg_cd,sub_catg_cd)

    if int(scenario_check) ==0:
        responseData ={"request_header":parameters,
                       "scenario":"Baseline"}
                
    else:
        responseData ={"request_header":parameters,
                       "scenario":"Return of inflation"}
        
        
    json_object = json.dumps(responseData)
    
    response = json_object

    return response


@application.route('/excel_summary_report', methods=['POST']) ##------ 60 ok
def func_excel_summary_report():
    print("inside func_excel_summary_report")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_report_V1.ul_db_excel_summary_report(content)
   
    response = json_string

    return response


@application.route('/v1/excel_summary_report', methods=['POST']) ##------ 61 ok
def func_excel_summary_report_new():
    print("inside func_excel_summary_report")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_report_V2.ul_db_excel_summary_report(content)
   
    response = json_string

    return response


@application.route('/excel_summary_report_volume', methods=['POST']) ##------ 62 ok 
def func_excel_summary_report_volume():
    print("inside func_excel_summary_report_volume")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_report_volume.ul_db_excel_summary_report_volume(content)
   
    response = json_string

    return response

@application.route('/excel_summary_by_channel', methods=['POST']) ##------ 63 ok
def func_excel_summary_report_channel():
    print("inside func_excel_summary_report_channel")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_channel_V1.ul_db_excel_summary_report(content)
   
    response = json_string

    return response

@application.route('/v1/excel_summary_by_channel', methods=['POST']) # ------- 64 ok
def func_excel_summary_report_channel_v1():
    print("inside func_excel_summary_report_channel")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_channel_V4.ul_db_excel_summary_report(content)
   
    response = json_string

    return response


@application.route('/driver_decomposition/volume_decomposition', methods=['POST']) # ------- 65 ok
def func_volume_decomposition():
    print("inside func_volume_decomposition")
    print(request.json)
    content = request.json
    geo_id = content['COUNTRY_NAME']
    division = content['DIVISION']
    catg_cd = content['CATAGORY_NAME']
    sub_catg_cd = content['SUB_CATEGORY_NAME']
    forecast_secenario = content['FORECAST_SCENARIO']
    
    json_string = ul_driver_decomposition.ul_db_volume_decomposition(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    response = json_string

    return response


@application.route('/driver_decomposition/volume_decomposition_qtr_change', methods=['POST']) ##---- 66 ok
def func_volume_decomposition_qtr_change():
    print("inside func_volume_decomposition_qtr_change")
    print(request.json)
    content = request.json
    geo_id = content['COUNTRY_NAME']
    division = content['DIVISION']
    catg_cd = content['CATAGORY_NAME']
    sub_catg_cd = content['SUB_CATEGORY_NAME']
    forecast_secenario = content['FORECAST_SCENARIO']
    
    
    json_string = ul_driver_decomposition.ul_db_volume_decomposition_qtr_change(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    response = json_string

    return response


@application.route('/driver_decomposition/volume_decomposition_qtr_growth', methods=['POST']) ##------ 67 ok
def func_volume_decomposition_qtr_growth():
    print("inside func_volume_decomposition_qtr_growth")
    print(request.json)
    content = request.json
    geo_id = content['COUNTRY_NAME']
    division = content['DIVISION']
    catg_cd = content['CATAGORY_NAME']
    sub_catg_cd = content['SUB_CATEGORY_NAME']
    forecast_secenario = content['FORECAST_SCENARIO']
    output = content['OUTPUT']
    
    json_string = ul_driver_decomposition.ul_db_volume_decomposition_qtr_pct_change(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,output)
   
    response = json_string

    return response


@application.route('/driver_decomposition/driver_by_quarter', methods=['POST']) ##---------68 xx
def func_driver_by_quarter():
    print("inside func_driver_by_quarter")
    print(request.json)
    content = request.json
    
    geo_id = content['COUNTRY_NAME']
    division = content['DIVISION']
    catg_cd = content['CATAGORY_NAME']
    sub_catg_cd = content['SUB_CATEGORY_NAME']
    forecast_secenario = content['FORECAST_SCENARIO']
    
    
    json_string = ul_driver_decomposition.ul_db_driver_by_quarter(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    response = json_string

    return response


@application.route('/driver_decomposition/driver_by_quarter_growth', methods=['POST']) ##-- 69 ok
def func_driver_by_quarter_growth():
    print("inside func_driver_by_quarter_growth")
    print(request.json)
    content = request.json
    
    geo_id = content['COUNTRY_NAME']
    division = content['DIVISION']
    catg_cd = content['CATAGORY_NAME']
    sub_catg_cd = content['SUB_CATEGORY_NAME']
    forecast_secenario = content['FORECAST_SCENARIO']
    
    json_string = ul_driver_decomposition.ul_db_driver_by_quarter_pct_growth(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario)
   
    response = json_string

    return response


# #@application.route('/driver_decomposition/driver_analysis', methods=['POST'])
# #def func_driver_analysis():
# #    print("inside func_driver_analysis")
# #    print(request.json)
# #    content = request.json
# #    
# #    geo_id = content['COUNTRY_NAME']
# #    division = content['DIVISION']
# #    catg_cd = content['CATAGORY_NAME']
# #    sub_catg_cd = content['SUB_CATEGORY_NAME']
# #    forecast_secenario = content['FORECAST_SCENARIO']
# #    driver = content['DRIVER']
# #    sub_driver = content['SUB_DRIVER']
# #    #sub_driver ="PRICE_PER_VOL" 
# #    json_string = ul_driver_decomposition.ul_db_driver_analysis(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,driver,sub_driver)
# #   
# #    response = json_string
# #
# #    return response

# #@application.route('/driver_decomposition/sub_driver_list', methods=['POST'])
# #def func_sub_driver_list():
# #    print("inside func_sub_driver_list")
# #    print(request.json)
# #    content = request.json
# #    
# #    geo_id = content['COUNTRY_NAME']
# #    division = content['DIVISION']
# #    catg_cd = content['CATAGORY_NAME']
# #    sub_catg_cd = content['SUB_CATEGORY_NAME']
# #    forecast_secenario = content['FORECAST_SCENARIO']
# #    driver = content['DRIVER']
# #    
# #    
# #    json_string = ul_driver_decomposition.ul_db_sub_driver_list(geo_id,division,catg_cd,sub_catg_cd,forecast_secenario,driver)
# #   
# #    response = json_string
# #
# #    return response


@application.route('/excel_summary_country_list', methods=['POST']) ##-- 70 ok
def func_excel_summary_country_list():
    print("inside func_excel_summary_country_list")
    #print(request.json)
    content = request.json
    json_string = ul_excel_summary_country_list.ul_db_excel_summary_country_list(content)
   
    response = json_string

    return response

@application.route('/catman_demo/heartbeat', methods=['GET']) ##-- 71 ok
def func_heartbeat():
    
    #print("inside func_heartbeat")
    #print(request.json)
    responseData ={"Response":"Success"}
        
    response = json.dumps(responseData)
  
    return response


#####################################################################################
#Main program
#####################################################################################

if __name__ == '__main__':
#Listening port: 4860
    application.run(debug=True, host='0.0.0.0',port=4860,threaded=True)
    #application.run(host='0.0.0.0', port=50100, debug=True)
    #application.run(debug=True, host='0.0.0.0',port=50100)
    

#####################################################################################
