


" India Total Level Prediction function "

import joblib
import pandas as pd
import numpy as np
#from pandas.core.common import SettingWithCopyWarning
#import warnings
#warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

pd.options.mode.chained_assignment = None


def Simulation_Function_India_Total(Data,price_flag,tdp_flag):
    
    DATAF = Data.copy(deep=True)
    DATAF["TDP_lag_1"] = DATAF["TDP"].shift(-1)
    DATAF["TDP_lag_1"] = DATAF["TDP_lag_1"].fillna(DATAF["TDP_lag_1"].mean())
    DATAF.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
    DATAF.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
    DATAF.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
    DATAF.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
    DATAF.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
    DATAF.rename(columns = {'GROCERY_AND_PHARMACY_PCT_CHANGE':'AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)'},inplace =True)
     
    if price_flag==0:
        
        " Price prediction "
        s = DATAF.shape[0]
    
        for i in range(s):
            SUBCAT = DATAF.SUBCAT_CD.iloc[i]
            CHNL= DATAF.CHNL_CD.iloc[i]
            FMT = DATAF.FMT_CD.iloc[i]
            REG = DATAF.UL_GEO_ID.iloc[i]
            
            if int(SUBCAT) == 18:
                INP_COL = ['MONTH','SEASONALITY_INDEX_PRICE','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(CONSUMER_PRICE_INDEX)']
            else:
                INP_COL = ['MONTH','SEASONALITY_INDEX_PRICE','AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(CONSUMER_PRICE_INDEX)']
        
            X = DATAF[INP_COL].copy()
            
            filename = './India_models/Price_Total_Prediction_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
          
            #load the model
            loaded_model = joblib.load(filename)
            
            YPred = loaded_model.predict(X)
            
            DATAF.PRICE_PER_VOL.iloc[i] = YPred[i]
        else:
            DATAF = DATAF.copy()
      
    
    if tdp_flag==0:    
        
        " TDP Prediction "
        
        s = DATAF.shape[0]
    
        for i in range(s):
            SUBCAT = DATAF.SUBCAT_CD.iloc[i]
            CHNL= DATAF.CHNL_CD.iloc[i]
            FMT = DATAF.FMT_CD.iloc[i]
            REG = DATAF.UL_GEO_ID.iloc[i]
            
            
            if int(SUBCAT) == 18:
                INP_COL = ['YEAR','MONTH','SEASONALITY_INDEX_TDP','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)']
            else:
                INP_COL = ['YEAR','SEASONALITY_INDEX_TDP','AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','AVG(RESIDENTIAL_PCT_CHANGE)']
        
            X = DATAF[INP_COL].copy()
            
            filename = './India_models/TDP_Total_Prediction_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
          
            #load the model from disk
            loaded_model = joblib.load(filename)
            
            YPred = loaded_model.predict(X)
            
            DATAF.TDP.iloc[i] = YPred[i]
        else:
            DATAF = DATAF.copy()
        
    
    
    " Sales Prediction "
    
    s = DATAF.shape[0]
    DATAF_OUT = DATAF[['PERIOD_ENDING_DATE','PRICE_PER_VOL']]
    DATAF_OUT.insert(1,'PREDICTED_VOLUME'," ")
    
    for i in range(s):
        SUBCAT = DATAF.SUBCAT_CD.iloc[i]
        CHNL= DATAF.CHNL_CD.iloc[i]
        FMT = DATAF.FMT_CD.iloc[i]
        REG = DATAF.UL_GEO_ID.iloc[i]
        
        
        if int(SUBCAT) == 18:
            INP_COL = ['SEASONALITY_INDEX_SALES','SALES_TREND_CAL','AVG(GDP_REAL_LCU)',
                   'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)',
                   'AVG(AVG_TEMP_CELSIUS)','PRICE_PER_VOL','TDP_lag_1']
            X = DATAF[INP_COL].values
        
            filename1 = './India_models/SALES_TOTAL_Prediction_XGB_'+str(int(SUBCAT))
            filename2 = './India_models/SALES_TOTAL_Prediction_LIN_'+str(int(SUBCAT))
      
            #load the model from
            loaded_model1 = joblib.load(filename1)
            loaded_model2 = joblib.load(filename2)
            
            YPred = (0.9*loaded_model1.predict(X).reshape(-1,1))+(0.1*loaded_model2.predict(X))
            DATAF_OUT.PREDICTED_VOLUME.iloc[i] = YPred[i].item(0)
        
        else:
            INP_COL = ['SALES_TREND_CAL','SEASONALITY_INDEX_SALES','PRICE_PER_VOL','TDP','AVG(AVG_TEMP_CELSIUS)','AVG(HUMID_PCT)','AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','AVG(RESIDENTIAL_PCT_CHANGE)','AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','AVG(UNEMP_RATE)']
    
            X = DATAF[INP_COL].values
            
            filename1 = './India_models/SALES_TOTAL_SUBCAT_Prediction_XGB_RERUN_MODELLING3_'+str(int(SUBCAT))
            filename2 = './India_models/SALES_TOTAL_SUBCAT_Prediction_LIN_RERUN_MODELLING3_'+str(int(SUBCAT))
          
            #load the model from
            loaded_model1 = joblib.load(filename1)
            loaded_model2 = joblib.load(filename2)
            
            #YPred = (0.8*loaded_model1.predict(X))+(0.2*loaded_model2.predict(X))
            YPred = (0.8*loaded_model1.predict(X).reshape(-1,1))+(0.2*loaded_model2.predict(X))
            DATAF_OUT.PREDICTED_VOLUME.iloc[i] = YPred[i].item(0)
            
            
    #DATAF.rename(columns = {'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)':'RETAIL_AND_RECREATION_PCT_CHANGE','AVG(RESIDENTIAL_PCT_CHANGE)':'RESIDENTIAL_PCT_CHANGE'},inplace =True)
    #DATAF.rename(columns = {'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)':'PERSONAL_DISPOSABLE_INCOME_REAL_LCU','AVG(UNEMP_RATE)':'UNEMP_RATE','AVG(CONSUMER_PRICE_INDEX)':'CONSUMER_PRICE_INDEX'},inplace =True)
    #DATAF.rename(columns = {'AVG(GDP_REAL_LCU)':'GDP_REAL_LCU'},inplace =True)
    #DATAF.rename(columns = {'AVG(UNEMP_RATE)':'UNEMP_RATE'},inplace =True)
    #DATAF.rename(columns = {'AVG(AVG_TEMP_CELSIUS)':'AVG_TEMP_CELSIUS','AVG(HUMID_PCT)':'HUMID_PCT'},inplace =True)
    
    return DATAF_OUT