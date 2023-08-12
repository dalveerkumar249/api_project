############ Netherlands Total Level Prediction function "
############ This program is used by simulator 
############ Written by Kalyan
########### 20-Dec-2021


import joblib
import pandas as pd



def Simulation_Function_Indonesia(DATAF,price_flag,tdp_flag):
    
    
    if DATAF.shape[0] !=0:
        DATAF.columns
        DATAF.index = range(len(DATAF))
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        price_cfg= pd.read_excel("./Indonesia_models_v1/Indonesia_Model_Variables.xlsx",sheet_name='PRICE')
        tdp_cfg= pd.read_excel("./Indonesia_models_v1/Indonesia_Model_Variables.xlsx",sheet_name='TDP')
        sales_cfg= pd.read_excel("./Indonesia_models_v1/Indonesia_Model_Variables.xlsx",sheet_name='SALES')
        
        price_cfg = price_cfg[(price_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        tdp_cfg = tdp_cfg[(tdp_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        sales_cfg = sales_cfg[(sales_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        
        DATAF1 = DATAF.copy(deep=True)
        DATAF2 = DATAF.copy(deep=True)
    
    
    #    DATAF.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
    #    DATAF.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
    #    DATAF.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
    #    DATAF.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
    #    DATAF.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
        
         
        if (price_flag==0):
    
               
            SUBCAT = DATAF1.SUBCAT_CD.iloc[0]
            CHNL= DATAF1.CHNL_CD.iloc[0]
            FMT = DATAF1.FMT_CD.iloc[0]
            REG = DATAF1.UL_GEO_ID.iloc[0]
            ####### reading price variables from excel
            INP_COL = price_cfg["IN_COL"].values[0]
            INP_COL = str(INP_COL)
    
            INP_COL = INP_COL.split(',')
            INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
            X1 = DATAF1[INP_COL]
            
            filename = './Indonesia_models_v1/PRICE_PER_VOL_xgb_and_lin_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
          
            #load the model
            loaded_model = joblib.load(filename)
            
            YPred1 = loaded_model.predict(X1)
            
            for i in range(0,len(DATAF)):
                DATAF["PRICE_PER_VOL"].values[i] = YPred1[i]
        else:
    
            DATAF = DATAF.copy()
          
        
        if tdp_flag==0:    
    
            
            
            SUBCAT = DATAF2.SUBCAT_CD.iloc[0]
            CHNL= DATAF2.CHNL_CD.iloc[0]
            FMT = DATAF2.FMT_CD.iloc[0]
            REG = DATAF2.UL_GEO_ID.iloc[0]
                
            INP_COL = tdp_cfg["IN_COL"].values[0]
            INP_COL = INP_COL.split(',')
            INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
            
            X2 = DATAF2[INP_COL]
            
                
            filename = './Indonesia_models_v1/TDP_xgb_and_lin_'+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
              
                #load the model from disk
            loaded_model = joblib.load(filename)
                
            YPred2 = loaded_model.predict(X2)
                
            for i in range(0,len(DATAF)):
                DATAF["TDP"].values[i] = YPred2[i]
    
        else:
            DATAF = DATAF.copy()
            
        
        
        " Sales Prediction "
        
        #print(DATAF.head())
        #DATAF_OUT = DATAF[['PERIOD_BEGIN_DATE']]
        DATAF_OUT = DATAF[['PERIOD_ENDING_DATE','PRICE_PER_VOL']]
        DATAF_OUT.insert(1,'PREDICTED_VOLUME'," ")
        
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        CHNL= DATAF.CHNL_CD.iloc[0]
        FMT = DATAF.FMT_CD.iloc[0]
        REG = DATAF.UL_GEO_ID.iloc[0]
        
        
        INP_COL = sales_cfg["IN_COL"].values[0]
        INP_COL = INP_COL.split(',')
        INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
    
    
        X = DATAF[INP_COL]
    
        #load the model from
        filename1 = "./Indonesia_models_v1/SALES_VOLUME_xgb_and_lin_"+str(int(SUBCAT))+"_"+str(int(CHNL))+"_"+str(int(FMT))+"_"+str(int(REG))
        loaded_model1 = joblib.load(filename1)
        
        price_lag = int(sales_cfg["Price_lag"].values[0])
        tdp_lag = int(sales_cfg["Tdp_lag"].values[0])
        
        
        X.PRICE_PER_VOL = X.PRICE_PER_VOL.shift(-price_lag)
        X.fillna(X.PRICE_PER_VOL.mean(),inplace = True)
        X.TDP = X.TDP.shift(-tdp_lag)
        X.fillna(X.TDP.mean(),inplace = True)
    
        YPred3 = loaded_model1.predict(X)
        for i in range(0,len(DATAF_OUT)):
            DATAF_OUT["PREDICTED_VOLUME"].values[i] = float(YPred3[i])
        
    
    return DATAF_OUT



'''
SUBCAT = 4

import joblib
import pandas as pd


testing_data = pd.read_csv('final_output_file_indonesia.csv')

testing_data.index = range(len(testing_data))
testing_data.columns
testing_data=testing_data[testing_data['SUBCAT_CD']==SUBCAT]
testing_data=testing_data[testing_data['SECTOR_SCENARIO_CD']==1]
DATAF = testing_data.copy(deep=True)
DATAF.columns

price_flag = 1
tdp_flag = 1
output = Simulation_Function_Indonesia(testing_data,0,0)


'''
