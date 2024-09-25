#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import re
from yandex_disk_func import *


# In[ ]:





# In[ ]:





# In[2]:


# Отдельно создаем функцию для парсинга досмотров по источнику One target
# на входе функция принимает только ссылку для скачивания таблицы
def get_one_target_views(data_link, report_type):
    cols_range = 'A:G' # задаем диапазон полей, которые нам нужны
    df = pd.read_excel(BytesIO(data_link), sheet_name='Отчет по всем РК', header=None, usecols=cols_range,
                   names=['product', 'impressions', '25', '50', '75', '100', 'VTR']) 
    df = df[['product', 'impressions', '25', '50', '75', '100']] # оставляем только нужные поля

    # на этом листе нет разбивки по дням
    # в самом верху таблицы присутствует строка с указанием периода загрузки
    # из этой строки мы заберем период и сохраним его в двух отдельных полях - начало и окончание периода
    period = df['product'][0] # забираем текст из ячейки, которая содержит период
    period = period.split('период ', 1)[1]
    start_period = period.split(' - ', 1)[0] # начало периода отчета
    end_period = period.split(' - ', 1)[1] # окончание периода отчета

    # теперь нам необходимо забрать статистику досмотров в разбивке по каждому продукту
    # для этого получим индекс строк, которые нужно сохранить
    df = df.fillna('') # заполняем пустые строки
    target_rows_list = list(df[df['product'].str.contains('Досмотры')].index) # создаем список индексов строк, которые содержат данные
    df = df.iloc[target_rows_list] # передаем список индексов, чтобы отфильтровать датаФрейм

    # теперь необходимо извлечь из ячейки название продукта
    df['product'] = df['product'].apply(lambda x: x.split('Досмотры ', 1)[1])
    # добавляем название источника, даты начала и окончания
    df['source'] = 'one target'
    df['start_period'] = start_period
    df['end_period'] = end_period
    df['product'] = df['product'].str.lower().str.strip()
    df['report_type'] = report_type
    return df


# In[1]:


# источник One target
# тип отчета Видео реклама
# Функция для обработки статистики по дням для обычных и бонусных размещений 
# Статистика по всем креативам на 2-м листе
# Каждый креатив в отдельной таблице
# Таблицы располагаются друг над другом
def get_one_target_video_base_bonus_report(data_link, report_type):
    tmp_video_dict = {}
    
    df = pd.read_excel(BytesIO(data_link), sheet_name='Отчет по дням', header=None)
    df = df.fillna('')

    # каждый продукт имеет свой диапазон строк
    # диапазон может отличаться (какие-то продукты запускаются раньше, какие-то позже)
    start_index_list = df[df[0]=='Дата'].index # забираем индекс строки для отсчета начала диапазона
    end_index_list = df[df[0]=='Итог'].index # забираем индекс строки для окончания диапазона

    df.columns = df.iloc[start_index_list[0]].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    # привоодим названия к единому стандарту
    df = df.rename(columns={'дата': 'date', 'показы': 'impressions', 'клики': 'clicks', 'охват': 'reach'}) 
    df = df[['date', 'impressions','reach', 'clicks']] # оставляем только нужные поля

    # теперь сформируем датаФрейм для каждого отдельного продукта
    # через цикл перебираем список индексов начала диапазона
    for i in range(len(start_index_list)):
        start_index = start_index_list[i] # берем индекс начала
        end_index = end_index_list[i] # берем индекс окончания
        # название продукта находится сверху таблицы с данными. поэтому нам нужна предыдущая ячейка перед начальным индексом
        product = str(df['date'][start_index-1]).lower.strip() # забираем название продукта
        # print(index_product_list[i])
        df_tmp = df.iloc[start_index+1:end_index] # забираем строки из диапазона

        df_tmp['source'] = 'one target'
        df_tmp['format_type'] = 'video'
        # df_tmp['product'] =  str(product).lower().strip()
        df_tmp['date'] = pd.to_datetime(df_tmp['date']).dt.date # приводим в формат даты
        # обязательно убираем дни, в которых не было показов
        # т.к. мы делим общее кол-во досмотров за период на кол-во дней в периоде
        # если отавить дни БЕЗ показов получится искаженная статистика
        df_tmp = df_tmp[df_tmp['impressions']!=0] # убираем дни, в которых не было показов объявлений
        # df_tmp['product'] =  df_tmp['product'].str.lower().str.strip()
        # сохраняем датаФрейм во временный словарь 
        # ключ - это название продукта (15s, 6s и тд)
        df_tmp['report_type'] = report_type # сохраняем в отдельном поле - относится отчет к бонусным или нет
        tmp_video_dict[product] = df_tmp
        
    return pd.concat(tmp_video_dict, ignore_index=True)


# In[ ]:


# источник Hybrid 
# тип отчета Видео и Баннерная реклама
# Функция для обработки статистики по дням для Видео и Баннерной рекламы в одном файле 
# Видео и Баннерная реклама на разных листах
def get_hybrid_video_banner_report(data_link, report_type, sheet_name):

    if sheet_name.lower()=='видео':
        df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name)
        # оставляем только нужные поля
        df = df[['Day', 'Tag', 'Impressions', 'Clicks', 'Video Views to 25%', 'Video Views to 50%', 
                 'Video Views to 75%', 'Video Views', 'Reach']]
        df = df.rename(columns={'Day': 'date', 'Tag': 'product', 'Impressions': 'impressions', 'Clicks': 'clicks', 
                        'Video Views to 25%': '25', 'Video Views to 50%': '50', 
                        'Video Views to 75%': '75', 'Video Views': '100', 
                        'Reach': 'reach'})
        df = df.fillna(0) # заполяем пустые строки, чтобы затем их удалить
        # забираем индекс строки для окончания диапазона
        first_blank_cell = list(df[df['date']==0].index)[0] 
        # оставляем только строки с данными, которые нам нужны
        df = df.iloc[:first_blank_cell]
        df['source'] = 'hybrid' #добавляем название источника
        df['format_type'] = 'video' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
        df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
        df['product'] = df['product'].str.lower().str.strip()
        df['report_type'] = report_type
        return df
        
    if 'баннер' in sheet_name.lower():
        df = pd.read_excel(BytesIO(data_link), sheet_name=sheet_name)
        # оставляем только нужные поля
        df = df[['Day', 'Tag', 'Impressions', 'Clicks', 'Reach']]
        df = df.rename(columns={'Day': 'date', 'Tag': 'product', 'Impressions': 'impressions', 'Clicks': 'clicks', 'Reach': 'reach'})
        
        df = df.fillna(0) # заполяем пустые строки, чтобы затем их удалить
        # забираем индекс строки для окончания диапазона
        first_blank_cell = list(df[df['date']==0].index)[0]
        # оставляем только строки с данными, которые нам нужны
        df = df.iloc[:first_blank_cell]
        df['source'] = 'hybrid' #добавляем название источника
        
        df['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
        df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
        df['product'] = df['product'].str.lower().str.strip()
        df['report_type'] = report_type
        return df


# In[ ]:


# источник Beeline
# тип отчета Видео реклама
# Функция для обработки статистики по дням для обычных размещений 
# Каждый креатив на отдельном листе

def get_beeline_video_report(data_link, report_type):
    tmp_video_dict = {}
        
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    cols_range = 'A:J' # задаем диапазон полей, которые нам нужны
    
     # проходим через цикл по списку названий листов
    for name in sheet_names.sheet_names:
        # передаем название листа для парсинга, диапазон колонок, которые нам нужны и заранее подготовленный списко названий полей
        df = pd.read_excel(BytesIO(data_link), sheet_name=name, usecols=cols_range, header=None, skiprows=8) 
        df = df.fillna(0)  #заполяем пустые строки, чтобы затем их удалить
        
        start_index_list = list(df[df[0]=='Date'].index)  # собираем список индексов, где есть название Date
    
        # Проходим через цикл по каждой таблице, которая содержится на листе
        for num, i in enumerate(start_index_list):
            # забираем индекс строки начала таблицы
            start_index = start_index_list[num] # берем индекс начала таблицы с данными
            creative_name = df[0][start_index-1] # забираем значение из ячейки над полем Дата 
            key = str(name).lower().strip() # ключ - это название листа excel, забираем на верхнем цикле
            if creative_name:
                key = name + '_' + str(creative_name).lower().strip() # если такое название есть, то добавляем его к базовому названию креатива
                
            df_tmp = df.iloc[start_index+1:]
            df_tmp.columns = df.iloc[start_index].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
            # привоодим названия к единому стандарту
            df_tmp = df_tmp.rename(columns={'first quartile': '25', 'midpoint': '50', 'third quartile': '75', 'complete views': '100'}) 
            df_tmp = df_tmp[['date', 'impressions','reach', 'clicks',  '25', '50', '75', '100']] # оставляем только нужные поля
            df_tmp = df_tmp.reset_index(drop=True) # сбрасываем индексацию
            end_index = list(df_tmp[df_tmp['date']==0].index)[0] # получаем окончание датаФрейма
            df_tmp = df_tmp.iloc[:end_index] # обрезаем датаФрейм до нужной строки
            df_tmp['date'] = pd.to_datetime(df_tmp['date']).dt.date # приводим датуВремя в просто дату
            df_tmp['source'] = 'beeline'
            df_tmp['format_type'] = 'video' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
            df_tmp['product'] = key # добавляем название продукта
            df_tmp['report_type'] = report_type
            # сохраняем датаФрейм во временный словарь 
            # ключ - это название продукта (15s, 6s и тд) + дополнительное название креатива(если оно есть)
    
            tmp_video_dict[key] = df_tmp  
        
    return pd.concat(tmp_video_dict, ignore_index=True)


# In[ ]:


# источник Gnezdo
# тип отчета Баннерная реклама
# Функция для обработки статистики по дням для обычных размещений 
# Каждый креатив на отдельном листе

def get_gnezdo_banner_report(data_link, report_type):
    tmp_banner_dict = {}
    
    cols_range = 'A:E' # задаем диапазон полей, которые нам нужны  
    sheet_names = pd.ExcelFile(BytesIO(data_link))
    # проходим через цикл по списку названий листов
    # sheets_list = sheet_names.sheet_names[1:]
    for name in sheet_names.sheet_names[1:]:
        # передаем название листа для парсинга, диапазон колонок, которые нам нужны и заранее подготовленный списко названий полей
        df = pd.read_excel(BytesIO(data_link), sheet_name=name, usecols=cols_range, header=None) 
        df = df.fillna(0)  #заполяем пустые строки, чтобы затем их удалить

        base_text = df[0][3].lower().strip() # Название креатива(продукта) в столбике А в 4 строке - должно начинаться со слова креатив
        base_text = re.sub('[_!;:()-]+', ' ', base_text ) # убираем из названия креатива лишние символы
        base_text = re.sub(' +', ' ', base_text) # e,bhftv kbiybt ghj,tks

        start_index = str(base_text).find('креатив') # Забираем индекс, с которого начинается название креатива
        product_name = str(base_text)[start_index:] # сохраняем название креатива

        start_index = list(df[df[0]=='Дата'].index)[0]  # берем индекс начала таблицы с данными
        end_index = list(df[df[0]=='Всего'].index)[0] # берем индекс окончания таблицы с данными

        df.columns = df.iloc[start_index].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
        df = df.iloc[start_index+1:end_index] # оставляем строки с данными, которые нам нужны
         # привоодим названия к единому стандарту
        df = df.rename(columns={'дата': 'date', 'охват': 'reach', 'показы': 'impressions', 'видимые показы': 'views', 'переходы': 'clicks'}) 
        df = df[['date', 'reach', 'impressions', 'views', 'clicks']] # оставляем только нужные поля
    
        df['source'] = 'gnezdo' #добавляем название источника
        df['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
        df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date().strftime('%Y-%m-%d'))# приводим в формат даты
        
        df['product'] = str(product_name).lower().strip() # добавляем статичное поле с названием продукта
        # df['product'] = df['product'].str.lower().str.strip()
        df['report_type'] = report_type
        # сохраняем датаФрейм во временный словарь 
        # ключ - это название продукта (15s, 6s и тд)
        tmp_banner_dict[name] = df

    return pd.concat(tmp_banner_dict, ignore_index=True)


# In[ ]:


# источник Astralab
# тип отчета Баннерная реклама
# Функция для обработки статистики по дням для обычных размещений 
# Источник НЕ отдает название креативов, поэтому мы просто пишем main
# Статистика по каждому месяцу находится на отдельном листе

def get_astralab_banner_report(data_link, report_type):
    tmp_banner_dict = {}

    sheet_names = pd.ExcelFile(BytesIO(data_link))
    end_index = sheet_names.sheet_names.index('Total') # Забираем индекс листа, на котором находится агрегированная статистика
   
    # проходим через цикл по списку названий листов БЕЗ учета листа Total
    for name in sheet_names.sheet_names[:end_index]:
        cols_range = 'A:E' # задаем диапазон полей, которые нам нужны
        df = pd.read_excel(BytesIO(data_link), sheet_name=name, usecols=cols_range)
        df = df.fillna(0)
        df = df.rename(columns={'Unnamed: 0': 'date', 'Clicks': 'clicks', 'Impressions': 'impressions', 'Reach': 'reach'})
        df = df[['date', 'reach', 'impressions', 'clicks']]
        
        product = 'main' # т.к. источник НЕ отдает название продукта - указываем для всех данных продукт main
        end_index = list(df[df['date']=='Total'].index)[0] # берем индекс окончания таблицы с данными
        
        df = df.iloc[:end_index] # оставляем строки с данными, которые нам нужны

        df['source'] = 'astralab' #добавляем название источника
        df['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
        df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
    
        df['product'] = str(product).lower().strip() # добавляем статичное поле с названием продукта
        # df['product'] = df['product'].str.lower().str.strip()
        df['report_type'] = report_type
        
        # сохраняем датаФрейм во временный словарь 
        # ключ - это название листа (Jul, Aug и тд)
        tmp_banner_dict[name] = df

    return pd.concat(tmp_banner_dict, ignore_index=True)


# In[ ]:


# источник Avito
# тип отчета Баннерная реклама
# Функция для обработки статистики по дням для обычных размещений 
# в источнике присутствует ИД креатива. Чтобы НЕ создавать отдельное поле в БД, 
# добавляем ИД креатива в конец его назвния через разделитель _id_идентификатор креатива
# Статистика в разбивке по дням находится на листе Statistics by creative

def get_avito_banner_report(data_link, report_type):
    
    df = pd.read_excel(BytesIO(data_link), sheet_name='Statistics by creative', skiprows=1)
    df = df[['ID Креатива', 'Дата', 'Название креатива', 'Показы', 'Клики', 'Охват']]
    df['product'] = df['Название креатива'] + '_id_' + df['ID Креатива'].astype('str')
    df = df[['Дата', 'product', 'Показы', 'Клики', 'Охват']]
    df = df.rename(columns={'Дата': 'date', 'Показы': 'impressions', 'Клики': 'clicks', 'Охват': 'reach'})
    
    df['source'] = 'avito' #добавляем название источника
    
    df['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
    df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
    df['product'] = df['product'].str.lower().str.strip()
    df['report_type'] = report_type
    return df


# In[ ]:


# источник turbotarget
# тип отчета Баннерная реклама
# Функция для обработки статистики по дням для обычных размещений 
# Статистика по креативам находится на листе - по креативам 
# название креатива находится в строке ПЕРЕД таблицей НАД полем дата
# шапка заголовка таблицы состоит из 2-х совмещенных строк
# каждая таблица заканчивается строкой Итого

def get_turbotarget_banner_report(data_link, report_type):
    tmp_video_dict = {}

    # создаем список с названиями полей
    cols_range = 'A:F'
    
    df = pd.read_excel(BytesIO(data_link), sheet_name='по креативам', header=None, usecols=cols_range)

    start_index_list = list(df[df[0]=='Дата'].index) # сохраняем в список индексы строк начала каждой таблицы с данными
    end_index_list = list(df[df[0]=='Итого'].index) # сохраняем в список индексы строк окончания каждой таблицы с данными

    df.columns = df.iloc[start_index_list[0]+1].str.lower().str.strip().str.replace('\n', ' ') # забираем название полей из файла
    df = df.rename(columns={np.nan: 'date', 'показы': 'impressions', 'клики': 'clicks', 'бюджет': 'budget', 'охват': 'reach'})
    df = df[['date', 'impressions', 'clicks', 'budget', 'reach']] # оставлем только нужные поля
    
    for i in range(len(start_index_list)):
        start_index = start_index_list[i] # получаем индкс начала конкретной таблицы
        end_index =  end_index_list[i]  # получаем индкс окончания конкретной таблицы
    
        name = str(df['date'][start_index-1]).lower().strip() # забираем название креатива, к которому относится статистика в таблице

        df_tmp = df.iloc[start_index+2:end_index] # оставляем строки с данными, которые нам нужны
        df_tmp = df_tmp.fillna('')
        df_tmp = df_tmp[df_tmp['impressions'] != '']
        df_tmp['product'] = name
        df_tmp['source'] = 'turbotarget' #добавляем название источника
        df_tmp['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
        df_tmp['date'] = pd.to_datetime(df_tmp['date']).dt.date  # приводим в формат даты

        df_tmp['report_type'] = report_type
        tmp_video_dict[name] = df_tmp

    return pd.concat(tmp_video_dict, ignore_index=True)


# In[ ]:


# источник mediaserfer
# тип отчета Баннерная реклама
# Функция для обработки статистики по дням для обычных размещений 
# Статистика в разбивке по дням находится на листе Отчет по креативам_дням

def get_mediaserfer_banner_report(data_link, report_type):
    df = pd.read_excel(BytesIO(data_link), sheet_name='Отчет по креативам_дням')
    df = df[['Дата', 'Креатив', 'Показы', 'Клики', 'Охват', 'Бюджет']]
    
    df = df.rename(columns={'Дата': 'date', 'Креатив': 'product','Показы': 'impressions', 
                            'Клики': 'clicks', 'Охват': 'reach', 'Бюджет': 'budget'})
    df['date'] = df['date'].fillna('0')
    
    end_index = list(df[df['date']=='0'].index)[0]
    df = df.iloc[:end_index] # оставляем строки с данными, которые нам нужны
    
    df['source'] = 'mediaserfer' #добавляем название источника
    
    df['format_type'] = 'banner' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)
    df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
    df['product'] = df['product'].str.lower()
    df['report_type'] = report_type

    return df


# In[ ]:


# источник digitalalliance
# тип отчета Видео реклама
# Функция для обработки статистики по дням для обычных размещений 
# Статистика в разбивке по дням находится на листе Sheet1
# Название креатива находится в поле В (Название РМ)
# Охват находится в поле К (Уники)

def get_digitalalliance_video_report(data_link, report_type):
    cols_range = 'B:K' # задаем диапазон полей, которые нам нужны

    # создаем список с названиями полей
    cols_name = ['product', 'date', 'impressions', 'clicks', 'ctr', '25', '50', '75', '100', 'reach']
    df = pd.read_excel(BytesIO(data_link), sheet_name='Sheet1', skiprows=2, usecols=cols_range, names=cols_name)
    df = df[['product', 'date', 'impressions', 'clicks', '25', '50', '75', '100', 'reach']]
    df = df.fillna(0)
    
    end_index = list(df[df['date']==0].index)[0]
    df = df.iloc[:end_index] # оставляем строки с данными, которые нам нужны
    
    df['source'] = 'digitalalliance' #добавляем название источника
    df['date'] = pd.to_datetime(df['date']).dt.date  # приводим в формат даты
    df['product'] = df['product'].str.lower()
    df['report_type'] = report_type
    df['format_type'] = 'video' # добавляем статичое поле с название Типа формата рекламы (Видео/Баннер)

    return df


# In[ ]:


# создаем функцию для обработки данных в эксель файле
# в зависимости от источника парсинг будет отличаться
# на входе функция принимает
# -название отчета - по сути это название источника
# - ссылку для скачивания эксель файла
# - путь к файлу, чтобы его удалить после закачивания
def parse_yandex_responce(report_name, data_link, main_folder, file_path, yandex_token, report_video_dict, report_banner_dict, report_video_views_dict):
    # некоторые отчеты содержат отдельно статистику по бонусным размещениям
    # для таких размещений на всякий случай создаем тип bonus 
    report_type = 'base'
    if 'bonus' in report_name:
        report_type = 'bonus'
        
    if report_name=='one_target_video' or report_name=='one_target_video_bonus':
        # На листе Отчет по дням содержится общая статистика в разбивке по дням
        # на листе Отчет по всем РК нас интересует статитсика досмотров (ее мы будем забирать через отдельную функцию)
        source_key = 'one_target'+ '_'+ report_type
        # вызываем функцию для обработки статистики по дням для обычных и бонусных размещений Видео рекламы
        report_video_dict[source_key] = get_one_target_video_base_bonus_report(data_link, report_type) 
    
        # отдельно вызываем функцию для добавления датаФрейма с общей таблицей досмотров
        report_video_views_dict[source_key] = get_one_target_views(data_link, report_type)

    if report_name=='hybrid_video_banner':
        source_key = 'hybrid' + '_'+ report_type
        sheet_names = pd.ExcelFile(BytesIO(data_link))
        for name in sheet_names.sheet_names:
            if name.lower()=='видео':
                report_video_dict[source_key] = get_hybrid_video_banner_report(data_link, report_type, name)
            if 'баннер' in name.lower():
                report_banner_dict[source_key] = get_hybrid_video_banner_report(data_link, report_type, name)

    if report_name=='beeline_video':
        source_key = 'beeline'+ '_'+ report_type
        report_video_dict[source_key] = get_beeline_video_report(data_link, report_type)

    if report_name=='gnezdo_banner':
        source_key = 'gnezdo' + '_'+ report_type
        report_banner_dict[source_key] = get_gnezdo_banner_report(data_link, report_type)

    if report_name=='astralab_banner':
        source_key = 'astralab' + '_'+ report_type
        report_banner_dict[source_key] = get_astralab_banner_report(data_link, report_type)

    if report_name=='avito_banner':
        source_key = 'avito' + '_'+ report_type
        report_banner_dict[source_key] = get_avito_banner_report(data_link, report_type)

    if report_name=='turbotarget_banner':
        source_key = 'turbotarget_' + '_'+ report_type
        report_banner_dict[source_key] = get_turbotarget_banner_report(data_link, report_type)

    if report_name=='mediaserfer_banner':
        source_key = 'mediaserfer_' + '_'+ report_type
        report_banner_dict[source_key] = get_mediaserfer_banner_report(data_link, report_type)

    if report_name=='digitalalliance_video':
        source_key = 'digitalalliance_' + '_'+ report_type
        report_video_dict[source_key] = get_digitalalliance_video_report(data_link, report_type)
        
    # в самом конце удаляем файл по этому источнику
    delete_yandex_disk_file(main_folder, file_path, yandex_token)


# In[ ]:


def get_data_from_ya_folder(yandex_folders, main_folder, file_path, yandex_token,
                                              report_video_dict, report_banner_dict, report_video_views_dict,
                           flag='new'):
    public_key = yandex_folders['public_key']  # из ответа Яндекс забираем public_key, чтобы использовать его для скачивания файлов

    for i in range(len(yandex_folders['_embedded']['items'])): # через цикл проходим по ответу Яндекса и забираем названия вложенных папок
        file_type = yandex_folders['_embedded']['items'][i]['type']
        if file_type=='dir':   # если находим файлы с типом dir (папка), то забираем путь к этой папке
            folder_path = yandex_folders['_embedded']['items'][i]['path']
            print(folder_path)
            if flag in folder_path:
                yandex_responce = get_yandex_disk_responce(base_public_url, public_key, folder_path) # отправляем запрос, чтобы получить содержимое папки
        
                # Через цикл проходим по папке с файлами
                # Нас интересуют файлы эксель. Причем каждая экселька будет парситься по своему, т.к. они относятся к разным рекламным площадкам
                
                # Проходим через цикл по содержимому папки (отдельный флайт)
                for i in range(len(yandex_responce['_embedded']['items'])):
                    file_info = yandex_responce['_embedded']['items'][i]
                    if file_info['type']=='file':  # если документ является фалйом(не папкой или изображением), то забираем его название 
                        file_name = file_info['name'] # сохраняем название файла
                        if 'xls' in file_name: # еслит тип файла является xlsx, то уберем расширение и будем его использовать в качесвте названия отчета
                            file_path = file_info['path']
                            
                            report_name = '.'.join(file_name.split('.')[:-1]) # убираем .xlsx из названия файла
                            print(report_name)
                            
                            res_file_link = get_yandex_disk_responce(download_url, public_key, file_path) # получаем ссылку на скачивание отчета
                            download_response = requests.get(res_file_link['href'])
                              
                            parse_yandex_responce(report_name, download_response.content, main_folder, file_path, yandex_token,
                                                  report_video_dict, report_banner_dict, report_video_views_dict)
                        
                    


# In[ ]:


# создаем функцию, чтобы посчитать кол-во дней от начала до конца периода
def get_count_days(row):
    max_date = datetime.strptime(str(row['max']), '%Y-%m-%d').date()
    min_date = datetime.strptime(str(row['min']), '%Y-%m-%d').date()
    count_days = (max_date - min_date).days
    return count_days + 1


# In[ ]:


# создаем функцию, чтобы расчитать кол-во досмотров в день
# это нужно, чтобы объединить данные от One target с данными из других источников
# все источники содержат разбивку досмотров по дням, кроме One target
# таким образом мы нормализуем данные

# делим общее кол-во досмотров по каждому продукту за период на кол-во дней в периоде и округляем до 2-х знаков после запятой
def get_views_by_day(row):
    qu_25 = round(row['25'] / row['count_days'], 2)
    qu_50 = round(row['50'] / row['count_days'], 2)
    qu_75 = round(row['75'] / row['count_days'], 2)
    qu_100 = round(row['100'] / row['count_days'], 2)
    return [qu_25, qu_50, qu_75, qu_100]


# In[ ]:


# создаем функцию для нормализации типов данных 
# эту функцию можем применить и к дф Видео и к Баннерам - т.к. поля везде назваются одинаково
def normalizeDataTypes(df, table_type='video'):
    df['source'] = df['source'].str.lower()
    df['product'] = df['product'].str.lower()
    df['report_type'] = df['report_type'].str.lower()

    
    if table_type=='video' or table_type=='banner':
        df['date'] = df['date'].astype('str')
        df['format_type'] = df['format_type'].astype('str')
        df['impressions'] = df['impressions'].astype('int64')
        df['clicks'] = df['clicks'].astype('int64')
        df['reach'] = df['reach'].astype('float64').round(2)
        df['budget'] = df['budget'].astype('float64')
        
    if table_type=='video' or table_type=='video_views':
        df['25'] = df['25'].astype('float64').round(2)
        df['50'] = df['50'].astype('float64').round(2)
        df['75'] = df['75'].astype('float64').round(2)
        df['100'] = df['100'].astype('float64').round(2)
        
    if table_type=='video_views':
        df['start_period'] = df['start_period'].astype('str')
        df['end_period'] = df['end_period'].astype('str')
        df['min_date'] = df['min_date'].astype('str')
        df['max_date'] = df['max_date'].astype('str')

    if table_type=='banner':
        df['views'] = df['views'].astype('int64')
        
    return df


# In[ ]:


# создаем функцию для сравнения последней даты(максимальной) из БД и дат, которые пришли в новых файлах статистики
# если дата в новой статистике меньше или равна максимальной дате из БД, стаим флаг 0
# иначе - это новая дата и ставим флаг=1
def get_clean_flag(row):
    flag = 1
    
    if row['max_db_date'] != '':
        new_date = datetime.strptime(str(row['date']), '%Y-%m-%d').date()
        old_date = datetime.strptime(str(row['max_db_date']), '%Y-%m-%d').date()
        if new_date <= old_date:
            flag = 0
    return flag


# In[ ]:


# создаем функцию, которая нормализует накопительную статистику по досмотрам
# если в новой статистик по Продукту НЕТ данных, то в поле min_date будет записан 0
# следовательно мы заполним поля с данными из стистики из БД
# иначе берем разницу между новыми накопленными данными и данными из БД
# таким образом можно понять сумму изменения в новом периоде
def normalize_data_video_views(row):
    min_date = row['min_date']
    max_date = row['max_date']
    increment_25 = row['25'] - row['video_views_25']
    increment_50 = row['50'] - row['video_views_50']
    increment_75 = row['75'] - row['video_views_75']
    increment_100 = row['100'] - row['video_views_100']

    # если для Источника-Продукта НЕ было изменений, то передаем последнюю актуальную статистику из БД
    if row['min_date']==0:
        min_date = row['min_db_date']
        max_date = row['max_db_date']
        increment_25 = row['video_views_25']
        increment_50 = row['video_views_50']
        increment_75 = row['video_views_75']
        increment_100 = row['video_views_100']

    return [min_date, max_date, increment_25, increment_50, increment_75, increment_100]


# In[ ]:





# In[ ]:




