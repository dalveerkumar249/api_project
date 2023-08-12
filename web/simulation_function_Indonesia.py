#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 19 14:46:01 2021

@author: pranav
"""
import joblib
import pandas as pd
import numpy as np

def seasonality_index_calculation_price(DATAF_in):
    DATAF_ALL_HIST_PRECOVID = DATAF_in[(DATAF_in.YEAR==2021) | (DATAF_in.YEAR==2022) | (DATAF_in.YEAR==2023)]
    Start_year = 2021
    Start_month = 7
    DATAF_ALL_START = DATAF_in[(DATAF_in.YEAR==Start_year) & (DATAF_in.MONTH==Start_month)]
    
    s = DATAF_ALL_START.shape[0]
    
    for i in range(s):
        #print(s)
        REG = DATAF_ALL_START.UL_GEO_ID.iloc[i]
        CHNL = DATAF_ALL_START.CHNL_CD.iloc[i]
        FMT = DATAF_ALL_START.FMT_CD.iloc[i]
        SUBCAT = DATAF_ALL_START.SUBCAT_CD.iloc[i]
        
        DATAF = DATAF_ALL_HIST_PRECOVID[(DATAF_ALL_HIST_PRECOVID.UL_GEO_ID==REG) & (DATAF_ALL_HIST_PRECOVID.CHNL_CD==CHNL) & (DATAF_ALL_HIST_PRECOVID.FMT_CD==FMT) & (DATAF_ALL_HIST_PRECOVID.SUBCAT_CD==SUBCAT)]
    
        OUT_COL = ['PRICE_PER_VOL']
    
        Y = DATAF[OUT_COL]
        
        " PRICE_PER_VOL DECOMPOSITION "
        import statsmodels.api as sm
        res = sm.tsa.seasonal_decompose(Y,freq=12)
    #    fig = res.plot()
    #    fig.set_figheight(8)
    #    fig.set_figwidth(15)
    #    plt.show()
        
        
        DATAF.insert(3, "SEASONALITY_INDEX_PRICE_PER_VOL", res.seasonal)
        
        DATAF_SEASONALITY = DATAF[(DATAF.YEAR==2019)]
        
        COL = ['UL_GEO_ID','CHNL_CD','FMT_CD','SUBCAT_CD','MONTH','SEASONALITY_INDEX_PRICE_PER_VOL']
        
        DATAF_SEASONALITY = DATAF_SEASONALITY[COL]
        
        
        if i==0:
            DATAF_ALL_SEASONALITY = DATAF_SEASONALITY
        else:
            DATAF_ALL_SEASONALITY = pd.concat([DATAF_ALL_SEASONALITY,DATAF_SEASONALITY])
    
    
    
    "-----------------------------------------------------------------------------------------------------------------------------"        
    " Incorporating Seasonality in the Dataframe "
    
    s = DATAF.shape[0]
    
    SEASONALITY_INDEX_PRICE_PER_VOL = np.zeros((s), dtype=float)
    
    DATAF_in.insert(2, "SEASONALITY_INDEX_PRICE_PER_VOL", SEASONALITY_INDEX_PRICE_PER_VOL)
    DATAF_in['SEASONALITY_INDEX_PRICE_PER_VOL'] = DATAF['SEASONALITY_INDEX_PRICE_PER_VOL']
    
    return DATAF_in
    


def seasonality_index_calculation_tdp(DATAF_in):
    DATAF_ALL_HIST_PRECOVID = DATAF_in[(DATAF_in.YEAR==2021) | (DATAF_in.YEAR==2022) | (DATAF_in.YEAR==2023)]
    Start_year = 2021
    Start_month = 7
    DATAF_ALL_START = DATAF_in[(DATAF_in.YEAR==Start_year) & (DATAF_in.MONTH==Start_month)]
    
    s = DATAF_ALL_START.shape[0]
    i = s
    for i in range(s):
        
        REG = DATAF_ALL_START.UL_GEO_ID.iloc[i]
        CHNL = DATAF_ALL_START.CHNL_CD.iloc[i]
        FMT = DATAF_ALL_START.FMT_CD.iloc[i]
        SUBCAT = DATAF_ALL_START.SUBCAT_CD.iloc[i]
        
        DATAF = DATAF_ALL_HIST_PRECOVID[(DATAF_ALL_HIST_PRECOVID.UL_GEO_ID==REG) & (DATAF_ALL_HIST_PRECOVID.CHNL_CD==CHNL) & (DATAF_ALL_HIST_PRECOVID.FMT_CD==FMT) & (DATAF_ALL_HIST_PRECOVID.SUBCAT_CD==SUBCAT)]
    
        OUT_COL = ['TDP']
    
        Y = DATAF[OUT_COL]
        
        " TDP DECOMPOSITION "
        import statsmodels.api as sm
        res = sm.tsa.seasonal_decompose(Y,freq=12)
    #    fig = res.plot()
    #    fig.set_figheight(8)
    #    fig.set_figwidth(15)
    #    plt.show()
        
        DATAF.insert(3, "SEASONALITY_INDEX_TDP", res.seasonal)
        #print(DATAF.columns)
        DATAF_SEASONALITY = DATAF[(DATAF.YEAR==2019)]
        
        COL = ['UL_GEO_ID','CHNL_CD','FMT_CD','SUBCAT_CD','MONTH','SEASONALITY_INDEX_TDP']
        
        DATAF_SEASONALITY = DATAF_SEASONALITY[COL]
        
        
        DATAF_ALL_SEASONALITY = DATAF_SEASONALITY
    
    
    "-----------------------------------------------------------------------------------------------------------------------------"        
    " Incorporating Seasonality in the Dataframe "
    
    s = DATAF.shape[0]
    
    SEASONALITY_INDEX_TDP = np.zeros((s), dtype=float)
    
    DATAF_in.insert(2, "SEASONALITY_INDEX_TDP", SEASONALITY_INDEX_TDP)
    
    DATAF_in['SEASONALITY_INDEX_TDP'] = DATAF['SEASONALITY_INDEX_TDP']
    
    return DATAF_in







'''
Data is a Dataframe of 24 rows which is generated by Ramesh.
'''
def simulation(Data,price_flag,tdp_flag,catdim):
    
    '''
    check the flags   
    '''
    DATAF_ALL = Data.copy(deep=True)
    DATAF_ALL_sales = Data.copy(deep=True)
    DATAF_ALL_sales.rename(columns = {'SEASONALITY_INDEX':'SEASONALITY_NEW'},inplace =True)
    DATAF_ALL.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)',
                                'RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)',
                                'CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)',
                                'SEASONALITY_INDEX':'SEASONALITY_NEW'},inplace =True)
    #print()
    if price_flag == 1:
        '''
        We don't predict price
        '''
    else:
        DATAF_ALL_PRICE = seasonality_index_calculation_price(DATAF_ALL)
        i = 0
        SUBCAT = DATAF_ALL_PRICE.SUBCAT_CD.iloc[i]
        CHNL= DATAF_ALL_PRICE.CHNL_CD.iloc[i]
        FMT = DATAF_ALL_PRICE.FMT_CD.iloc[i]
        REG = DATAF_ALL_PRICE.UL_GEO_ID.iloc[i]
        
#        print("Price")
#        print("SUBCAT ",SUBCAT)
#        print("CHANNEL ",CHNL)
#        print("FORMAT ",FMT)
#        print("REGION ",REG)
#        
        
        
        INP_COL = ['YEAR','MONTH','SEASONALITY_INDEX_PRICE_PER_VOL','AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(CONSUMER_PRICE_INDEX)']
        
        #OUT_COL = ['PRICE_PER_VOL']
        
            
        filename = './Indonesia_total_level/PRICE_Prediction_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
        model = joblib.load(filename)
        #from sklearn.model_selection import cross_val_score
              
        DATAF = DATAF_ALL_PRICE[(DATAF_ALL.SUBCAT_CD==SUBCAT) & (DATAF_ALL.CHNL_CD==CHNL)]
        
        DATAF = DATAF[(DATAF.FMT_CD==FMT) & (DATAF.UL_GEO_ID==REG)]
        # print(DATAF_ALL.columns)
        X = DATAF[INP_COL]
        #Y = DATAF[OUT_COL]
        # print(X.shape,"PRICE shape")
        YPred_Full_PRICE = model.predict(X)
        
        DATAF_ALL_sales['PRICE_PER_VOL'] = YPred_Full_PRICE
        
        
#        plt.figure(figsize=(20,6))
#        plt.plot(Y.values)
#        plt.plot(YPred_Full)
#        plt.show()
#        
        
        
    if tdp_flag == 1:
        '''
        We don't predict TDP
        '''
    else:
        DATAF_ALL_TDP = seasonality_index_calculation_tdp(DATAF_ALL)
        i = 0
        SUBCAT = DATAF_ALL_TDP.SUBCAT_CD.iloc[i]
        CHNL= DATAF_ALL_TDP.CHNL_CD.iloc[i]
        FMT = DATAF_ALL_TDP.FMT_CD.iloc[i]
        REG = DATAF_ALL_TDP.UL_GEO_ID.iloc[i]
    
#        print("TDP")
#        print("SUBCAT ",SUBCAT)
#        print("CHANNEL ",CHNL)
#        print("FORMAT ",FMT)
#        print("REGION ",REG)
        
        
        INP_COL = ['MONTH','SEASONALITY_INDEX_TDP','AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','AVG(RESIDENTIAL_PCT_CHANGE)']
        
        #OUT_COL = ['TDP']
        
        filename = './Indonesia_total_level/TDP_Prediction_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
        model = joblib.load(filename)
        
        #from sklearn.model_selection import cross_val_score
        
              
        DATAF = DATAF_ALL_TDP[(DATAF_ALL.SUBCAT_CD==SUBCAT) & (DATAF_ALL.CHNL_CD==CHNL)]
        
        DATAF = DATAF_ALL_TDP[(DATAF.FMT_CD==FMT) & (DATAF.UL_GEO_ID==REG)]
        
        X = DATAF[INP_COL]
        #print(X.shape,"XXXXXXXXXXXX")
        #Y = DATAF[OUT_COL]
        YPred_Full_TDP = model.predict(X)
#        lis_of_tdp_values.append(YPred_Full)
#        
#        plt.figure(figsize=(20,6))
#        plt.plot(Y.values)
#        plt.plot(YPred_Full)
#        plt.show()
 
        DATAF_ALL_sales['TDP'] = YPred_Full_TDP
    
    
    dir_name = './' + "Indonesia_models"
    file_name = catdim + "_.dat"
    full_file_name = dir_name +'/'+ file_name

    saved_model = joblib.load(full_file_name)
    #print(DATAF_ALL_sales.shape,"DATAF_ALL_sales")
    DATAF_ALL_sales = DATAF_ALL_sales[['YEAR','MONTH','SEASONALITY_NEW', 'HOLIDAY_CD', 'PRICE_PER_VOL',
                                        'TDP', 'AVG_TEMP_CELSIUS', 'RETAIL_AND_RECREATION_PCT_CHANGE',
                                        'RESIDENTIAL_PCT_CHANGE', 'PERSONAL_DISPOSABLE_INCOME_REAL_LCU',
                                        'GDP_NOMINAL_LCU', 'UNEMP_RATE', 'PREF_VALUE', 'COVID_FLAG']]
    predictions = saved_model.predict(DATAF_ALL_sales.values)
    #print(predictions)
    
    df = pd.DataFrame()
    df['PERIOD_ENDING_DATE'] = DATAF_ALL['PERIOD_ENDING_DATE']
    df['PREDICTED_VOLUME'] = predictions
    df['PRICE_PER_VOL'] = DATAF_ALL['PRICE_PER_VOL']
    
    
    '''
    Output df should have 2 columns 1 is date, 2nd is sales predictions
    '''
    return df


#DATAF_ALL_HIST_PROJ_SALES = pd.read_csv('sample_input_values_sim_indonesia.csv')
#outDF = simulation(DATAF_ALL_HIST_PROJ_SALES,0,0,'catdim346')
#outDF.to_csv("model_output2.csv")