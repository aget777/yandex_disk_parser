#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from io import BytesIO
import requests
from urllib.parse import urlencode
import urllib
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError
import os
import json
import yadisk
from datetime import datetime, date, timedelta
import locale
from time import sleep
import shutil
import gc
import turbodbc
from turbodbc import connect
import gc
from pandas.api.types import is_string_dtype
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import warnings


# In[ ]:


# создаем функцию для удаления таблицы в БД
# если скрипт требует полной перезаписи данных, то сначала удаляем таблицу в БД с помощью этой функции
# а затем сохраняем таблицу с новыми данными

def dropTable(table_name_db, conn, cursor):

    sql = f"""IF EXISTS(SELECT *
              FROM   [dbo].{table_name_db})
      DROP TABLE [dbo].{table_name_db}"""
    
    cursor.execute(sql)
    conn.commit()
    
    print(f'Таблица: {table_name_db} успешно удалена в БД: {database_local}')
    print('#' * 10)


# In[ ]:


# создаем таблицы через Быструю загрузку и определяем тип данных для каждого поля 
# на входе наша функция принимает
# - название таблицы, под которым она будет записана в БД
# - список названий полей с типом данных 
# - тип таблицы (video / banner) - от этого зависит кол-во полей
# - флаг (create / drop) - создать таблицу с нуля / удалить старую таблицу и создать таблицу заново

def reCreateDBTable(server_local, database_local, table_name_db, vars_list, table_type='video', flag='create'):
    conn = connect(driver="SQL Server",
          server=server_local,
          port="1433",
          database=database_local,
         # turbodbc_options=options
                  )
    
    cursor = conn.cursor()

    if flag=='drop':
        try:
            dropTable(table_name_db, conn, cursor)
        except:
            print(f'Таблицы {table_name_db} не существует в БД {server_local}')
        
    vars_string = ', '.join(str(elem) for elem in vars_list)
    
    if table_type=='video':
        vars_string = f'{vars_string}, video_views_25 float, video_views_50 float, video_views_75 float, video_views_100 float'
        
    if table_type=='banner':
        vars_string = f'{vars_string}, views int'
    try:
        sql =  f"""
             IF NOT EXISTS 
         (SELECT * FROM sysobjects 
         WHERE id = object_id(N'[dbo].[{table_name_db}]') AND 
         OBJECTPROPERTY(id, N'IsUserTable') = 1) 
         CREATE TABLE [dbo].[{table_name_db}] (
            {vars_string}
         )
    """
    
        cursor.execute(sql)
        conn.commit()

    
    except:
        print(f'Ошибка в файле {table_name_db}')
        print(exception)
    
    conn.close()
    cursor.close()    
    print(f'Пустая таблица {table_name_db} успешно создана в БД {database_local}')


# In[ ]:


# заливаем таблицы в БД
# функция на входе принимает датаФрейм с данными и название таблицы, в которую записать данные

def downloadTableToDB(server_local, database_local, table_name_db, df):
    conn = connect(driver="SQL Server",
          server=server_local,
          port="1433",
          database=database_local,
         # turbodbc_options=options
                  )
    cursor = conn.cursor()
    
    if cursor:
        print('Connect success')
        
    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}') 
    
    
    try:
    
        values = [np.ma.MaskedArray(df[col].values, pd.isnull(df[col].values)) for col in df.columns]
        colunas = '('
        colunas += ', '.join(df.columns)
        colunas += ')'
    
        val_place_holder = ['?' for col in df.columns]
        sql_val = '('
        sql_val += ', '.join(val_place_holder)
        sql_val += ')'
    
        sql = f"""
        INSERT INTO {table_name_db} {colunas}
        VALUES {sql_val}
        """
    
        cursor.executemanycolumns(sql, values)
        conn.commit()
    
        
    #         df.drop(df.index, inplace=True)
        print(f'Данные добавлены в БД: {database_local}, таблица: {table_name_db}')
    
    except:
        print(f'Ошибка в файле {table_name_db}')
        print(exception)
    
    conn.close()
    cursor.close()    
        
         
    finish_time = datetime.now()
    print(f'Скрипт отработал {finish_time}')
    
    print(f'Время выполнения задачи: {finish_time - start_time}')
    print(f'Загрузка завершена. Данные успешно добавлены в БД: {database_local}')
    print('#' * 50)
    print()


# In[ ]:


# создаем функцию для загрузки данных в БД
# которые НЕ требуют строгого определения типов данных
# в основном она используется для загрузки справочников и небольших таблиц

# на входе принимаем строку подключения / словарь с датаФреймами для загрузки / флаг добавления или перезаписи

def baseDownloadDBTable(server_local, database_local, df, table_name_db, flag='append'):
    conn_str_local = 'DRIVER={SQL Server};SERVER='+server_local+';DATABASE='+database_local+';'
    # создаем подключение к нашей локальной БД
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str_local}', use_setinputsizes=False)
    # проверяем подключение
    if engine:
        print('Connect success')

    # создаем отсчет времени на загрузку
    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}') 

    try:
        # при записи таблицы передаем флаг - replace / append - перезаписываем или добавляем данные
        df.to_sql(table_name_db, engine, if_exists=flag, index=False, method='multi', chunksize=100)
        engine.dispose() # разрываем соединение
        
        print(f'Данные добавлены в БД: {database_local}, таблица: {table_name_db}')

    except:
        print(f'Ошибка в файле {table_name_db}')

    # считаем время, которое потребовалось на загрузку данных
    finish_time = datetime.now()
    print(f'Скрипт отработал {finish_time}')
    
    print(f'Время выполнения задачи: {finish_time - start_time}')
    print(f'Загрузка завершена. Данные успешно добавлены в БД: {database_local}')

