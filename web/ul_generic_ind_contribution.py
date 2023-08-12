# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 11:55:44 2021

@author: Rameshkumar
"""
import pandas as pd

def ul_db_generic_ind_contribution(masterDF,segment):
   
    growthrateDict ={"YEAR":[],"PERIOD_TYPE":[],segment:[],"VOLUME_GROWTH":[]}
    masterDF["VOLUME"] = masterDF["VOLUME"].astype(float)
    tempDF = masterDF.copy()
    tempDF[["YEAR","MONTH"]] = tempDF["YEAR_QTR"].str.split("-",expand=True)
    tempDF["YEAR"] = tempDF["YEAR"].astype(int)
    starting_year = min(tempDF[tempDF['MONTH']=='Q1']['YEAR'])+1
    year_list = list(tempDF[tempDF['MONTH']=='Q1']['YEAR'].unique())
    
    type_list = list(tempDF[segment].unique()) 
    pivotDF = pd.pivot_table(masterDF,index=[segment],columns='YEAR_QTR',values='VOLUME').reset_index()
    
    for year in year_list:
        HY1 = str(year)+'-HY1'
        HY2 = str(year)+'-HY2'
        YEAR = str(year)+'-YEAR'
        try:
            pivotDF[HY1] = pivotDF[str(year)+'-Q1']+pivotDF[str(year)+'-Q2']
        except:
            pivotDF[HY1] = 0
        try:
            pivotDF[HY2] = pivotDF[str(year)+'-Q3']+pivotDF[str(year)+'-Q4']
        except:
            pivotDF[HY2] = 0
            
        pivotDF[YEAR] = pivotDF[HY1] + pivotDF[HY2]
    
    
    
    prev_year = starting_year -1
    year = starting_year
    
    
    
    for item in type_list:
            for year in year_list:
                if year >= starting_year:
                    prev_year = year -1                
                    try:
                        total = pivotDF[str(year)+'-Q1'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-Q1'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-Q1'].values[0]
                        grow_rate = round((value/total)*100,2)
                    except:
                        grow_rate = 0
                        
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('Q1')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                                    
                    try:
                        total = pivotDF[str(year)+'-Q2'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-Q2'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-Q2'].values[0]
                        grow_rate = round((value/total)*100,2)
                    except:
                        grow_rate = 0
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('Q2')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                    
                    try:
                        total = pivotDF[str(year)+'-Q3'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-Q3'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-Q3'].values[0]
                        grow_rate = round((value/total)*100,2)
                    except:
                        grow_rate = 0
                    
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('Q3')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                    
                    try:
                        total = pivotDF[str(year)+'-Q4'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-Q4'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-Q4'].values[0]
                        grow_rate = round((value/total)*100,2)
                    except:
                        grow_rate = 0
                        
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('Q4')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                    
                    try:
                        total = pivotDF[str(year)+'-HY1'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-HY1'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-HY1'].values[0]
                        grow_rate = round((value/total)*100,2) if (total!= 0) else 0 
                    except:
                        grow_rate = 0
                        
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('HY1')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                    
                    try:
                        total = pivotDF[str(year)+'-HY2'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-HY2'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-HY2'].values[0]
                        grow_rate = round((value/total)*100,2) if (total!= 0) else 0
                    
                    except:
                        grow_rate = 0
                    
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('HY2')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
                    
                    try:
                        total = pivotDF[str(year)+'-YEAR'].sum()
                        value = pivotDF.loc[pivotDF[segment]==item,str(year)+'-YEAR'].values[0] - pivotDF.loc[pivotDF[segment]==item,str(prev_year)+'-YEAR'].values[0]
                        grow_rate = round((value/total)*100,2)
                    except:
                        grow_rate = 0
                        
                    growthrateDict["YEAR"].append(year)
                    growthrateDict["PERIOD_TYPE"].append('YEAR')
                    growthrateDict[segment].append(item)
                    growthrateDict["VOLUME_GROWTH"].append(grow_rate)
    
    
    finalDF = pd.DataFrame(growthrateDict)
    return finalDF


