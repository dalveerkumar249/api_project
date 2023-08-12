############ SA Total Level Prediction function "
############ This program is used by simulator 
############ Written by Kalyan
########### 2-Nov-2021


import joblib
import pandas as pd



def Simulation_Function_SA_Total(data,price_flag,tdp_flag):
    
    DATAF = data.copy(deep=True)
    DATAF.index = range(len(DATAF))
    SUBCAT = DATAF.SUBCAT_CD.iloc[0]
    price_cfg= pd.read_excel("./sa_models/SA_Model_Variables.xlsx",sheet_name='PRICE')
    tdp_cfg= pd.read_excel("./sa_models/SA_Model_Variables.xlsx",sheet_name='TDP')
    sales_cfg= pd.read_excel("./sa_models/SA_Model_Variables.xlsx",sheet_name='SALES')
    model_cfg= pd.read_excel("./sa_models/SA_Model_Variables.xlsx",sheet_name='MODEL_NAME')
    
    price_cfg = price_cfg[(price_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
    tdp_cfg = tdp_cfg[(tdp_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
    sales_cfg = sales_cfg[(sales_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)
    model_cfg = model_cfg[(model_cfg["SUBCAT_CD"]==SUBCAT)].reset_index(drop=True)

    DATAF.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
    DATAF.rename(columns = {'PERSONAL_DISPOSABLE_INCOME_REAL_LCU':'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)','UNEMP_RATE':'AVG(UNEMP_RATE)','CONSUMER_PRICE_INDEX':'AVG(CONSUMER_PRICE_INDEX)'},inplace =True)
    DATAF.rename(columns = {'GDP_REAL_LCU':'AVG(GDP_REAL_LCU)'},inplace =True)
    DATAF.rename(columns = {'UNEMP_RATE':'AVG(UNEMP_RATE)'},inplace =True)
    DATAF.rename(columns = {'AVG_TEMP_CELSIUS':'AVG(AVG_TEMP_CELSIUS)','HUMID_PCT':'AVG(HUMID_PCT)'},inplace =True)
    
     
    if (price_flag==0):
        
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        CHNL= DATAF.CHNL_CD.iloc[0]
        FMT = DATAF.FMT_CD.iloc[0]
        REG = DATAF.UL_GEO_ID.iloc[0]
        ####### reading price variables from excel
        INP_COL = price_cfg["IN_COL"].values[0]
        INP_COL = INP_COL.split(',')
        INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
        X = DATAF[INP_COL]
        
        filename = './sa_models/Price_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
      
        #load the model
        loaded_model = joblib.load(filename)
        
        YPred = loaded_model.predict(X)
        
        for i in range(0,len(DATAF)):
            DATAF["PRICE_PER_VOL"].values[i] = YPred[i]
    else:

        DATAF = DATAF.copy()
      
    
    if tdp_flag==0:    
    
        SUBCAT = DATAF.SUBCAT_CD.iloc[0]
        CHNL= DATAF.CHNL_CD.iloc[0]
        FMT = DATAF.FMT_CD.iloc[0]
        REG = DATAF.UL_GEO_ID.iloc[0]
            
        INP_COL = tdp_cfg["IN_COL"].values[0]
        INP_COL = INP_COL.split(',')
        INP_COL = list(filter(lambda x:x.replace("'",""),INP_COL))
        
        X = DATAF[INP_COL]
            
        filename = './sa_models/TDP_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
          
            #load the model from disk
        loaded_model = joblib.load(filename)
            
        YPred = loaded_model.predict(X)
            
        for i in range(0,len(DATAF)):
            DATAF["TDP"].values[i] = YPred[i]

    else:
        DATAF = DATAF.copy()
        
    
    
    " Sales Prediction "
    
    #print(DATAF.head())
    DATAF_OUT = DATAF[['PERIOD_ENDING_DATE','PRICE_PER_VOL']]
    DATAF_OUT.insert(1,'PREDICTED_VOLUME'," ")
    
    
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
    
    if RE_NEED =="Y":
        YPred = (0.8*loaded_model1.predict(X).reshape(-1,1))+(0.2*loaded_model2.predict(X))
    else:
        YPred = (0.8*loaded_model1.predict(X))+(0.2*loaded_model2.predict(X))

    for i in range(0,len(DATAF_OUT)):
        DATAF_OUT["PREDICTED_VOLUME"].values[i] = float(YPred[i])

            
    DATAF.rename(columns = {'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)':'RETAIL_AND_RECREATION_PCT_CHANGE','AVG(RESIDENTIAL_PCT_CHANGE)':'RESIDENTIAL_PCT_CHANGE'},inplace =True)
    DATAF.rename(columns = {'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)':'PERSONAL_DISPOSABLE_INCOME_REAL_LCU','AVG(UNEMP_RATE)':'UNEMP_RATE','AVG(CONSUMER_PRICE_INDEX)':'CONSUMER_PRICE_INDEX'},inplace =True)
    DATAF.rename(columns = {'AVG(GDP_REAL_LCU)':'GDP_REAL_LCU'},inplace =True)
    DATAF.rename(columns = {'AVG(UNEMP_RATE)':'UNEMP_RATE'},inplace =True)
    DATAF.rename(columns = {'AVG(AVG_TEMP_CELSIUS)':'AVG_TEMP_CELSIUS','AVG(HUMID_PCT)':'HUMID_PCT'},inplace =True)
    
    return DATAF_OUT



'''

testing_data = pd.read_csv('SA_Total_Level_FR_Data_All_Scenarios.csv')

testing_data = pd.read_csv('SA_Total_Level_Data_All_Scenarios _minus_6_10_11.csv')
testing_data = pd.read_csv('SA_Total_Level_ToiletCleaners_Data_All_Scenarios.csv')

testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_SALES_SUBCAT_4_7_8_SCENARIO_1.csv')

testing_data.index = range(len(testing_data))
testing_data.columns
testing_data=testing_data[testing_data['SUBCAT_CD']==4]
testing_data=testing_data[testing_data['SECTOR_SCENARIO_CD']==1]
testing_data=testing_data[-24:]
DATAF = testing_data.copy(deep=True)
DATAF.columns

price_flag = 1
tdp_flag = 1

output = Simulation_Function_SA_Total(testing_data,1,1)



'''
