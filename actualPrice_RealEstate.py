# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
import glob
import pandas as pd

import json

from datetime import timedelta

# --------------------------------------------------------------------------------
# Create functions for three steps process, step1 - fn_readData, step2 - fn_combineData, step3 - fn_filterstatistics.
# 1, Read files (from different folders) as dataframes,
#     and transform these dataframes by add column df_name,
#     finally write these dataframes as csv files to a temp location(TempData folder).
#    Return value - tempPath, transfer to next step function to be used as path variable.
#
# 2, Read files (temp folder path from XCom) as dataframes,
#     and combine all dataframes into a big dataframe, named df_all,
#     finally write df_all as a csv file to a temp location(TempData folder).
#    Return value - file, this XCom value is transferred to next step function to be used as file variable.
#
# 3, Read file (named df_all.csv) as dataframe,
#     and use this dataframe to produce two results.
#     a, filter dataframe with three conditions ('住家用', '住宅大樓', '大於等於十三層')
#        , and write to csv file, named filter.csv
#     b, calculate four descriptive statistics ('總件數', '總車位數', '平均總價元', '平均車位總價元')
#        , and write to csv file, named count.csv
#
# --------------------------------------------------------------------------------

def fn_readData(**kwargs):
    # List all folders below source_folder
    path = "/home/may/Documents/RealEstate/SourceData/"

    # Get all folders in SourceData folder
    folders = os.listdir(path)

    file_dict = {}
    
    for f in folders:
        # Create absolute path
        fullpath = os.path.join(path, f)

        # Change working directory for each seasonal data
        if os.path.isdir(fullpath):
            os.chdir(fullpath)

            # Create a list to store files in each seasonal folder
            file_extension = ".csv"
            realEstate = "*_lvr_land_*" + file_extension

            all_reFilenames = [i for i in glob.glob(f"*{realEstate}")]


            # Create a dictionary to store every seasonal dataset, with keys storing informational filename, values storing datasets
            for file in all_reFilenames:
                key = f + "_" + file.replace('.csv','')
                try:
                    data = pd.read_csv(file, header=1, error_bad_lines=False, usecols=range(0,28))
                    data.columns = data.columns.str.replace(' ', '_')
                    data.insert(loc=0, column="df_name", value=f.replace("S","_") + "_" + file.replace(".csv","").replace("_lvr_land",""))
                    file_dict[key] = data
                    
                    tempPath = "/home/may/Documents/RealEstate/TempData/"
                    tempData = data
                    tempData.to_csv(tempPath + key + file_extension, index=False, encoding="utf_8_sig")
                    
                except pd.errors.EmptyDataError:
                    print("file:" + key + " is empty")
                    
    return tempPath
    


def fn_combineDataset(**kwargs):
    ti = kwargs['ti']
    tempPath = ti.xcom_pull(task_ids='readData')
    print(tempPath)
    os.chdir(tempPath)
    all_tempFilenames = [i for i in glob.glob("*_lvr_land_*.csv")]
    df_all = pd.concat([pd.read_csv(file, header=0) for file in all_tempFilenames])
    file = tempPath + "df_all.csv"
    df_all.to_csv(file, index=False, encoding="utf_8_sig")

    return file


def fn_filterstatistics(**kwargs):

    # Filter dataframe with condition
    file = kwargs['ti'].xcom_pull(task_ids='combineData')
    df_all = pd.read_csv(file, header=0)
    df_all["total_floor_number"] = df_all["total_floor_number"].astype(str)

    condition1 = df_all.main_use.str.contains("住家用", na=False)
    condition2 = df_all.building_state.str.contains("住宅大樓", na=False)
    condition3 = ["一層","二層","三層","四層","五層","六層","七層","八層","九層","十層","十一層","十二層"
              ,"1","2","3","4","5","6","7","8","9","10","11","12"]
    filter = df_all[condition1 & condition2]
    filter = filter[~filter["total_floor_number"].isin(condition3)]

    filter.to_csv("/home/may/Documents/RealEstate/Output/filter.csv", index=False, encoding="utf_8_sig")


    ## Calculate values ##

    # 總件數
    totCount = df_all["serial_number"].count()
    print("總件數: ", totCount)

    # 總車位數
    condition = df_all.transaction_sign.str.contains("車位", na=False)
    df_car = pd.to_numeric(df_all["transaction_pen_number"].str.split("車位").str[1])
    carCount = df_car.sum()
    print("總車位數: ", carCount)

    # 平均總價元
    avgPrice = df_all["total_price_NTD"].mean()
    print("平均總價元: ", avgPrice)

    # 平均車位總價元
    condition = df_all.transaction_sign.str.contains("車位", na=False)
    avgCarPrice = df_all["the_berth_total_price_NTD"].sum() / carCount
    print("平均車位總價元: ", avgCarPrice)

    count = pd.DataFrame(data={"總件數" : [totCount], "總車位數" : [carCount], "平均總價元" : [avgPrice], "平均車位總價元" : [avgCarPrice]})
    count.to_csv("/home/may/Documents/RealEstate/Output/count.csv", index=False, encoding="utf_8_sig")





# --------------------------------------------------------------------------------
# set default arguments
# --------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('actualPrice_realEstate', default_args=default_args, render_template_as_native_obj=True,
    schedule_interval="@daily")

# --------------------------------------------------------------------------------
# This task should read folders from source folder (106S1~109S4),
# and for each folder, it can read csv files with rule, *_lvr_land_*.csv,
# then transform these datasets, load these datasets as file name rule, eg.106S1_A_lvr_land_A,
# to a temp location (TempData folder)
# --------------------------------------------------------------------------------

readData = PythonOperator(
    task_id='readData',
    python_callable=fn_readData,
    dag=dag)

# --------------------------------------------------------------------------------
# Read files in TempData folder with file name rule, *_lvr_land_*.csv,
# and combine all datasets into a big dataset,
# then load this big dataset (df_all) to TempData folder, named df_all.csv
# --------------------------------------------------------------------------------

combineDataset = PythonOperator(
    task_id='combineData',
    python_callable=fn_combineDataset,
    dag=dag)

combineDataset.set_upstream(readData)

# --------------------------------------------------------------------------------
# Read file df_all.csv from folder TempData
# , and filter and count dataset with some conditions
# --------------------------------------------------------------------------------

filterNstatistics = PythonOperator(
    task_id='filterNstatistics',
    python_callable=fn_filterstatistics,
    dag=dag)

filterNstatistics.set_upstream(combineDataset)



