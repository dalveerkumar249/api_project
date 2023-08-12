############ Netherlands Total Level Prediction function "
############ This program is used by simulator 
############ Written by Kalyan
########### 20-Dec-2021


import joblib
import pandas as pd



def Simulation_Function_NTH_Total(datadf,price_flag,tdp_flag):
    
    DATAF = datadf.copy()
    DATAF.index = range(len(DATAF))
    season = []
    DATAF['MONTH'] = DATAF['MONTH'].astype(int)
    for row in DATAF['MONTH']:
        if row == 1 : season.append(4)
        elif row ==2 : season.append(1)
        elif row ==3: season.append(1)
        elif row ==4 : season.append(1)
        elif row ==5: season.append(2)
        elif row ==6: season.append(2)
        elif row ==7: season.append(2)
        elif row ==8: season.append(3)
        elif row ==9: season.append(3)
        elif row ==10: season.append(3)
        elif row ==11: season.append(4)
        elif row ==12: season.append(4)
    DATAF['SEO']=season
    
    a3=DATAF['MONTH']
    DATAF= pd.get_dummies(DATAF , columns= ['MONTH'], drop_first = False)
    DATAF['MONTH']=a3


    SUBCAT = DATAF.SUBCAT_CD.iloc[0]
    price_cfg= pd.read_excel("./nl_models/NTH_Model_Variables.xlsx",sheet_name='PRICE')
    tdp_cfg= pd.read_excel("./nl_models/NTH_Model_Variables.xlsx",sheet_name='TDP')
    sales_cfg= pd.read_excel("./nl_models/NTH_Model_Variables.xlsx",sheet_name='SALES')
    model_cfg= pd.read_excel("./nl_models/NTH_Model_Variables.xlsx",sheet_name='MODEL_NAME')
    
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

        DATAF1.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
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
        
        filename = './nl_models/Price_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
      
        #load the model
        loaded_model = joblib.load(filename)
        
        YPred1 = loaded_model.predict(X1)
        
        for i in range(0,len(DATAF)):
            DATAF["PRICE_PER_VOL"].values[i] = YPred1[i]
    else:

        DATAF = DATAF.copy()
      
    
    if tdp_flag==0:    

        DATAF2.rename(columns = {'RETAIL_AND_RECREATION_PCT_CHANGE':'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)','RESIDENTIAL_PCT_CHANGE':'AVG(RESIDENTIAL_PCT_CHANGE)'},inplace =True)
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
            
        filename = './nl_models/TDP_Total_Prediction_'+str(SUBCAT)+"_"+str(CHNL)+"_"+str(FMT)+"_"+str(REG)
          
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
    
    if SUBCAT in [2,5]:
        w1 = 0.6
        w2 = 0.4
    else: 
        w1 = 0.6
        w2 = 0.4
        
    if RE_NEED =="Y":
        YPred = (w1*loaded_model1.predict(X).reshape(-1,1))+(w2*loaded_model2.predict(X))
    else:
        YPred = (w1*loaded_model1.predict(X))+(w2*loaded_model2.predict(X))

    for i in range(0,len(DATAF_OUT)):
        DATAF_OUT["PREDICTED_VOLUME"].values[i] = float(YPred[i])

            
#    DATAF.rename(columns = {'AVG(RETAIL_AND_RECREATION_PCT_CHANGE)':'RETAIL_AND_RECREATION_PCT_CHANGE','AVG(RESIDENTIAL_PCT_CHANGE)':'RESIDENTIAL_PCT_CHANGE'},inplace =True)
#    DATAF.rename(columns = {'AVG(PERSONAL_DISPOSABLE_INCOME_REAL_LCU)':'PERSONAL_DISPOSABLE_INCOME_REAL_LCU','AVG(UNEMP_RATE)':'UNEMP_RATE','AVG(CONSUMER_PRICE_INDEX)':'CONSUMER_PRICE_INDEX'},inplace =True)
#    DATAF.rename(columns = {'AVG(GDP_REAL_LCU)':'GDP_REAL_LCU'},inplace =True)
#    DATAF.rename(columns = {'AVG(UNEMP_RATE)':'UNEMP_RATE'},inplace =True)
#    DATAF.rename(columns = {'AVG(AVG_TEMP_CELSIUS)':'AVG_TEMP_CELSIUS','AVG(HUMID_PCT)':'HUMID_PCT'},inplace =True)
    
    return DATAF_OUT



'''

import joblib
import pandas as pd

testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_SALES_SCENARIO_ALL_2_5.csv')

testing_data = pd.read_csv('DATAF_ALL_HIST_PROJ_SALES_SCENARIO_ALL_1_3_4_6_7.csv')


testing_data.index = range(len(testing_data))
testing_data.columns
testing_data=testing_data[testing_data['SUBCAT_CD']==1]
testing_data=testing_data[testing_data['SECTOR_SCENARIO_CD']==1]
testing_data=testing_data[testing_data['RECORD_TYPE']=="FORECAST"]
DATAF = testing_data.copy(deep=True)
DATAF.columns

price_flag = 1
tdp_flag = 1

output = Simulation_Function_NTH_Total(testing_data,0,0)



'''
