############ Netherlands Total Level Prediction function "
############ This program is used by simulator 
############ Written by Kalyan
########### 20-Dec-2021


import joblib
import pandas as pd



def Simulation_Function_CHN_Total(dataDF1,dataDF2,price_flag,tdp_flag):
    
    DATAF_offline = dataDF1.copy()
    DATAF_online = dataDF2.copy()
    DATAF_OUT = pd.DataFrame()
    
    if DATAF_offline.shape[0] !=0:
        DATAF = DATAF_offline
        DATAF.columns
        DATAF.index = range(len(DATAF))
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        price_cfg= pd.read_excel("./cn_models/China_Model_Variables.xlsx",sheet_name='PRICE')
        tdp_cfg= pd.read_excel("./cn_models/China_Model_Variables.xlsx",sheet_name='TDP')
        sales_cfg= pd.read_excel("./cn_models/China_Model_Variables.xlsx",sheet_name='SALES')
        model_cfg= pd.read_excel("./cn_models/China_Model_Variables.xlsx",sheet_name='MODEL_NAME')
        
        price_cfg = price_cfg[(price_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        tdp_cfg = tdp_cfg[(tdp_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        sales_cfg = sales_cfg[(sales_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        model_cfg = model_cfg[(model_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
    
        DATAF1 = DATAF.copy(deep=True)
        DATAF2 = DATAF.copy(deep=True)
    
    
    #    DATAF.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
    #    DATAF.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
    #    DATAF.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
    #    DATAF.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
    #    DATAF.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
        
         
        if (price_flag==0):
    
            if SUBCAT !=7:
                DATAF1.rename(columns = {'TRAFFIC_WEIGHT':'AVG(TRAFFIC_WEIGHT)'},inplace =True)
                DATAF1.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
                DATAF1.rename(columns = {'GDP_NOMINAL_LCU':'AVG(GDP_NOMINAL_LCU)'},inplace =True)
        
                DATAF1.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
                DATAF1.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
                DATAF1.rename(columns = {'SHARE_PRICE_INDEX':'AVG(SHARE_PRICE_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'RETAIL_PRICES_INDEX':'AVG(RETAIL_PRICES_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'GROCERY_AND_PHARMACY_PCT_CHANGE':'AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)'},inplace =True)
                DATAF2.rename(columns = {'NEW_DEATHS':'SUM(NEW_DEATHS)'},inplace =True)
                DATAF2.rename(columns = {'NEW_CASES':'SUM(NEW_CASES)'},inplace =True)
        
            SUBCAT = int(DATAF1.SUBCAT_CD.iloc[0])
            CHNL= int(DATAF1.CHNL_CD.iloc[0])
            FMT = int(DATAF1.FMT_CD.iloc[0])
            REG = int(DATAF1.UL_GEO_ID.iloc[0])
            ####### reading price variables from excel
            INP_COL = price_cfg["IN_COL"].values[0]
            INP_COL = str(INP_COL)
    
            INP_COL = INP_COL.split(',')
            INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
            X1 = DATAF1[INP_COL]
            
            filename = './cn_models/offline_models/Price_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
          
            #load the model
            loaded_model = joblib.load(filename)
            
            YPred1 = loaded_model.predict(X1)
            
            for i in range(0,len(DATAF)):
                DATAF["PRICE_PER_VOL"].values[i] = YPred1[i]
        else:
    
            DATAF = DATAF.copy()
          
        
        if tdp_flag==0:    
           
            if SUBCAT !=7:
                DATAF2.rename(columns = {'TRAFFIC_WEIGHT':'AVG(TRAFFIC_WEIGHT)'},inplace =True)
                DATAF2.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
                DATAF2.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
                DATAF2.rename(columns = {'GDP_NOMINAL_LCU':'AVG(GDP_NOMINAL_LCU)'},inplace =True)
        
                DATAF2.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
                DATAF2.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
                DATAF2.rename(columns = {'SHARE_PRICE_INDEX':'AVG(SHARE_PRICE_INDEX)'},inplace =True)
                DATAF2.rename(columns = {'RETAIL_PRICES_INDEX':'AVG(RETAIL_PRICES_INDEX)'},inplace =True)
                DATAF2.rename(columns = {'GROCERY_AND_PHARMACY_PCT_CHANGE':'AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)'},inplace =True)
                DATAF2.rename(columns = {'NEW_DEATHS':'SUM(NEW_DEATHS)'},inplace =True)
                DATAF2.rename(columns = {'NEW_CASES':'SUM(NEW_CASES)'},inplace =True)
        
            
            SUBCAT = int(DATAF2.SUBCAT_CD.iloc[0])
            CHNL= int(DATAF2.CHNL_CD.iloc[0])
            FMT = int(DATAF2.FMT_CD.iloc[0])
            REG = int(DATAF2.UL_GEO_ID.iloc[0])
                
            INP_COL = tdp_cfg["IN_COL"].values[0]
            INP_COL = INP_COL.split(',')
            INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
            
            X2 = DATAF2[INP_COL]
                
            filename = './cn_models/offline_models/TDP_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
              
                #load the model from disk
            loaded_model = joblib.load(filename)
                
            YPred2 = loaded_model.predict(X2)
                
            for i in range(0,len(DATAF)):
                DATAF["TDP"].values[i] = YPred2[i]
    
        else:
            DATAF = DATAF.copy()
            
        
        
        " Sales Prediction "
        
        #print(DATAF.head())
        DATAF_OUT = DATAF[['PERIOD_ENDING_DATE','PRICE_PER_VOL']]
        DATAF_OUT.insert(2,'SALES_VOLUME_PREDICTED_offline'," ")
        DATAF_OUT.rename(columns = {'PRICE_PER_VOL':'PRICE_PER_VOL_offline'},inplace =True)
        
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        CHNL= DATAF.CHNL_CD.iloc[0]
        FMT = DATAF.FMT_CD.iloc[0]
        REG = DATAF.UL_GEO_ID.iloc[0]
        
        
        INP_COL = sales_cfg["IN_COL"].values[0]
        INP_COL = INP_COL.split(',')
        INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
    
    
        X = DATAF[INP_COL].values
    
        RE_NEED = model_cfg["RESHAPE_NEEDED"].values[0]
        filename1 = eval(model_cfg["IN_FILE_NAME_1"].values[0])
        filename2 = eval(model_cfg["IN_FILE_NAME_2"].values[0])
        #load the model from
        loaded_model1 = joblib.load(filename1)
        loaded_model2 = joblib.load(filename2)
        
        if SUBCAT in [1,9,13,12,5]:
            w1 = 0.95
            w2 = 0.05
        elif SUBCAT == 7:
            w1 = 0.8
            w2 = 0.2
        else:#[2,6,10,11]
            w1 = 0.5
            w2 = 0.5
            
            
        if RE_NEED =="Y":
            YPred = (w1*loaded_model1.predict(X).reshape(-1,1))+(w2*loaded_model2.predict(X))
        else:
            YPred = (w1*loaded_model1.predict(X))+(w2*loaded_model2.predict(X))
    
        for i in range(0,len(DATAF_OUT)):
            DATAF_OUT["SALES_VOLUME_PREDICTED_offline"].values[i] = float(YPred[i])

    

    if DATAF_online.shape[0] !=0:
        DATAF = DATAF_online
        DATAF.columns
        DATAF.index = range(len(DATAF))
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        price_cfg= pd.read_excel("./cn_models/China_Model_Variables_online.xlsx",sheet_name='PRICE')
        tdp_cfg= pd.read_excel("./cn_models/China_Model_Variables_online.xlsx",sheet_name='TDP')
        sales_cfg= pd.read_excel("./cn_models/China_Model_Variables_online.xlsx",sheet_name='SALES')
        model_cfg= pd.read_excel("./cn_models/China_Model_Variables_online.xlsx",sheet_name='MODEL_NAME')
        
        price_cfg = price_cfg[(price_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        tdp_cfg = tdp_cfg[(tdp_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        sales_cfg = sales_cfg[(sales_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
        model_cfg = model_cfg[(model_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
    
        DATAF1 = DATAF.copy(deep=True)
        DATAF2 = DATAF.copy(deep=True)
    
    
    #    DATAF.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
    #    DATAF.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
    #    DATAF.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
    #    DATAF.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
    #    DATAF.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
        
         
        if (price_flag==0):
            
            if SUBCAT !=4:
                
                DATAF1.rename(columns = {'TRAFFIC_WEIGHT':'AVG(TRAFFIC_WEIGHT)'},inplace =True)
                DATAF1.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
                DATAF1.rename(columns = {'GDP_NOMINAL_LCU':'AVG(GDP_NOMINAL_LCU)'},inplace =True)
        
                DATAF1.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
                DATAF1.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
                DATAF1.rename(columns = {'SHARE_PRICE_INDEX':'AVG(SHARE_PRICE_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'RETAIL_PRICES_INDEX':'AVG(RETAIL_PRICES_INDEX)'},inplace =True)
                DATAF1.rename(columns = {'GROCERY_AND_PHARMACY_PCT_CHANGE':'AVG(GROCERY_AND_PHARMACY_PCT_CHANGE)'},inplace =True)
                DATAF2.rename(columns = {'NEW_DEATHS':'SUM(NEW_DEATHS)'},inplace =True)
                DATAF2.rename(columns = {'NEW_CASES':'SUM(NEW_CASES)'},inplace =True)
        
            SUBCAT = int(DATAF1.SUBCAT_CD.iloc[0])
            CHNL= int(DATAF1.CHNL_CD.iloc[0])
            FMT = int(DATAF1.FMT_CD.iloc[0])
            REG = int(DATAF1.UL_GEO_ID.iloc[0])
            ####### reading price variables from excel
            INP_COL = price_cfg["IN_COL"].values[0]
            INP_COL = str(INP_COL)
    
            INP_COL = INP_COL.split(',')
            INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
            X1 = DATAF1[INP_COL]
            
            filename = './cn_models/online_models/Price_Total_Prediction_'+str(SUBCAT)+"_"+str(6)+"_"+str(FMT)+"_"+str(REG)
          
            #load the model
            loaded_model = joblib.load(filename)
            
            YPred1 = loaded_model.predict(X1)
            
            for i in range(0,len(DATAF)):
                DATAF["PRICE_PER_VOL"].values[i] = YPred1[i]
        else:
    
            DATAF = DATAF.copy()
        
        
        
        
        " Sales Prediction "
        
        #print(DATAF.head())
        #DATAF_OUT = DATAF[['PERIOD_BEGIN_DATE']]
        if DATAF_OUT.shape[0]!=0:
            DATAF_OUT.insert(3,'SALES_VOLUME_PREDICTED_online'," ")
            DATAF_OUT.insert(4,'PRICE_PER_VOL',DATAF["PRICE_PER_VOL"])
            DATAF_OUT.rename(columns = {'PRICE_PER_VOL':'PRICE_PER_VOL_online'},inplace =True)
        else:
            DATAF_OUT = DATAF[['PERIOD_ENDING_DATE','PRICE_PER_VOL']]
            DATAF_OUT.insert(2,'SALES_VOLUME_PREDICTED_online'," ")
            DATAF_OUT.rename(columns = {'PRICE_PER_VOL':'PRICE_PER_VOL_online'},inplace =True)
        
        SUBCAT = int(DATAF.SUBCAT_CD.iloc[0])
        CHNL= int(DATAF.CHNL_CD.iloc[0])
        FMT = int(DATAF.FMT_CD.iloc[0])
        REG = int(DATAF.UL_GEO_ID.iloc[0])
        
        if int(SUBCAT) ==3:
            DATAF.PRICE_PER_VOL = DATAF.PRICE_PER_VOL.shift(-1)
            DATAF.fillna(DATAF.PRICE_PER_VOL.mean(),inplace = True)
            
        
        INP_COL = sales_cfg["IN_COL"].values[0]
        INP_COL = INP_COL.split(',')
        INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
    
    
        X = DATAF[INP_COL].values
    
        RE_NEED = model_cfg["RESHAPE_NEEDED"].values[0]
        filename1 = eval(model_cfg["IN_FILE_NAME_1"].values[0])
        filename2 = eval(model_cfg["IN_FILE_NAME_2"].values[0])
        #load the model from
        loaded_model1 = joblib.load(filename1)
        loaded_model2 = joblib.load(filename2)
        
        
        if SUBCAT in [2,10,11,13]:
            w1 = 0.5
            w2 = 0.5
        elif SUBCAT in [1,6,9]:#[2,6,10,11]
            w1 = 0.2
            w2 = 0.8
        elif SUBCAT == 4:#[2,6,10,11]
            w1 = 0.05
            w2 = 0.95
        else:
            w1 =0.95
            w2 =0.05
                
            
        if RE_NEED =="Y":
            YPred = (w1*loaded_model1.predict(X).reshape(-1,1))+(w2*loaded_model2.predict(X))
        else:
            YPred = (w1*loaded_model1.predict(X))+(w2*loaded_model2.predict(X))
    
        for i in range(0,len(DATAF_OUT)):
            DATAF_OUT["SALES_VOLUME_PREDICTED_online"].values[i] = float(YPred[i])

        
#    DATAF.rename(columns = {'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)':'RETAIL_AND_RECREATION_PCT_CHANGE','AVG(RESIDENTIAL_PCT_CHANGE)':'RESIDENTIAL_PCT_CHANGE'},inplace =True)
#    DATAF.rename(columns = {'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)':'PERSONAL_DISPOSABLE_INCOME_REAL_LCU','AVG(UNEMP_RATE)':'UNEMP_RATE','AVG(CONSUMER_PRICE_INDEX)':'CONSUMER_PRICE_INDEX'},inplace =True)
#    DATAF.rename(columns = {'AVG(GDP_REAL_LCU)':'GDP_REAL_LCU'},inplace =True)
#    DATAF.rename(columns = {'AVG(UNEMP_RATE)':'UNEMP_RATE'},inplace =True)
#    DATAF.rename(columns = {'AVG(AVG_TEMP_CELSIUS)':'AVG_TEMP_CELSIUS','AVG(HUMID_PCT)':'HUMID_PCT'},inplace =True)
    
    if int(SUBCAT) in [1,2,6,9,10,11,13]:
        DATAF_OUT["PREDICTED_VOLUME"] = DATAF_OUT["SALES_VOLUME_PREDICTED_offline"]+DATAF_OUT["SALES_VOLUME_PREDICTED_online"]
        DATAF_OUT["PREDICTED_VALUE"] = DATAF_OUT["SALES_VOLUME_PREDICTED_offline"] * DATAF_OUT["PRICE_PER_VOL_offline"] + DATAF_OUT["SALES_VOLUME_PREDICTED_online"] * DATAF_OUT["PRICE_PER_VOL_online"]
        DATAF_OUT.drop(["SALES_VOLUME_PREDICTED_offline","SALES_VOLUME_PREDICTED_online"],axis =1,inplace=True)
        DATAF_OUT.drop(["PRICE_PER_VOL_offline","PRICE_PER_VOL_online"],axis =1,inplace=True)
   
    elif int(SUBCAT) in [3,4]:
        DATAF_OUT["PREDICTED_VOLUME"] = DATAF_OUT["SALES_VOLUME_PREDICTED_online"]
        DATAF_OUT["PREDICTED_VALUE"] =  DATAF_OUT["SALES_VOLUME_PREDICTED_online"] * DATAF_OUT["PRICE_PER_VOL_online"]
        DATAF_OUT.drop(["SALES_VOLUME_PREDICTED_online"],axis =1,inplace=True)
        DATAF_OUT.drop(["PRICE_PER_VOL_online"],axis =1,inplace=True)
    
    else:
        DATAF_OUT["PREDICTED_VOLUME"] = DATAF_OUT["SALES_VOLUME_PREDICTED_offline"]
        DATAF_OUT["PREDICTED_VALUE"] = DATAF_OUT["SALES_VOLUME_PREDICTED_offline"] * DATAF_OUT["PRICE_PER_VOL_offline"] 
        DATAF_OUT.drop(["SALES_VOLUME_PREDICTED_offline",],axis =1,inplace=True)
        DATAF_OUT.drop(["PRICE_PER_VOL_offline"],axis =1,inplace=True)
    
    
    return DATAF_OUT



'''
SUBCAT = 4

import joblib
import pandas as pd

if SUBCAT in [1]:
         testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_TOTALSALES_29_NOV_MAYO.csv')
elif SUBCAT in [2]: 
    testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_TOTALSALES_29_NOV_BOUILLON.csv')
elif SUBCAT in [3]: 
    testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_TOTALSALES_29_NOV_MINI_MEALS.csv')
else:
    testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_TOTALSALES_29_NOV_DRY_SAUCE.csv')

testing_data = pd.read_csv('all_online_data_for_upload_batch_1.csv')

testing_data.index = range(len(testing_data))
testing_data.columns
testing_data=testing_data[testing_data['SUBCAT_CD']==2]
testing_data=testing_data[testing_data['SECTOR_SCENARIO_CD']==1]
DATAF = testing_data.copy(deep=True)
DATAF.columns

price_flag = 1
tdp_flag = 1
dummy =pd.DataFrame()
output = Simulation_Function_CHN_Total(dummy,testing_data,0,0)


'''
