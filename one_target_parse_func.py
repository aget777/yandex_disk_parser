#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from datetime import datetime, date, timedelta
from pandas.api.types import is_string_dtype
import numpy as np
from parse_custom_report import *


# In[ ]:


def get_first_one_target_data(one_target_df, one_target_views_df):
    if one_target_df.empty:
        return pd.DataFrame()
   
    # группируем датаФрейм по названию продукта и забираем минимальную и максимальную дату
    df = one_target_df.groupby(['source', 'product', 'report_type']).agg(['min', 'max'])['date'].reset_index()

    df['count_days'] = df.apply(get_count_days, axis=1) # считаем кол-во дней между мин макс датой
    df = df.rename(columns={'min': 'min_date', 'max': 'max_date'}) # приводим названия полей в порядок

    # к таблице, которая содержит досмотры слева по названию продукта присоединяем сгруппированную таблицу
    # таким образом для каждого продукта мы получаем 
    # - кол-во досмотров / мин и макс дату статистики / кол-во дней, которое рекламируется этот продукт
    one_target_views_df = one_target_views_df.merge(df, how='left', left_on=['source', 'product', 'report_type'], 
                                                                    right_on=['source', 'product', 'report_type'])

    # считаем кол-во досмотров в день по каждому продукту
    one_target_views_df['25_per_day'] = one_target_views_df.apply(lambda x: get_views_by_day(x)[0], axis=1)
    one_target_views_df['50_per_day'] = one_target_views_df.apply(lambda x: get_views_by_day(x)[1], axis=1)
    one_target_views_df['75_per_day'] = one_target_views_df.apply(lambda x: get_views_by_day(x)[2], axis=1)
    one_target_views_df['100_per_day'] = one_target_views_df.apply(lambda x: get_views_by_day(x)[3], axis=1)

    # к таблице, в которой содержится статистика в разбивке по дням
    # через левое соединение по полю Продукт мы присоеднияем досмотры в день
    # таким образом унас получается  таблица, которая содержит полную статистику
    # теперь мы сможем объединить данные из всех источников в одну таблицу
    one_target_df = one_target_df.merge(one_target_views_df[['source', 'product', 'report_type', 
                                                             '25_per_day', '50_per_day', '75_per_day', '100_per_day']],
                                        how='left', left_on=['source', 'product', 'report_type'], right_on=['source', 'product', 'report_type'])

    
    # переименовываем поля, чтобы к ним было легче обращаться (меньше букв)
    one_target_df = one_target_df.rename(columns={'25_per_day': '25', '50_per_day': '50', '75_per_day': '75', '100_per_day': '100'})

    one_target_views_df = one_target_views_df[['source', 'product', 'start_period', 'end_period', 'min_date', 'max_date', 
                                                                              '25', '50', '75', '100',  'report_type']]

    return one_target_df, one_target_views_df


# In[ ]:


def get_anoter_download_one_target_data(one_target_df, one_target_views_df, db_video_views_df):
    if one_target_df.empty:
        return pd.DataFrame()

    # теперь расчитаем досмотры в разбивке по дням для новых дат по источнику one target
    # создаем отдельный датаФрейм, чтобы понять для каких продуктов появилась новая стаитстика и сколько дней она содержит
    df = one_target_df[['date', 'source', 'product', 'report_type']]
    
    # группируем датаФрейм по названию продукта и забираем минимальную и максимальную дату
    df = df.groupby(['source', 'product', 'report_type']).agg(['min', 'max'])['date'].reset_index()
    df = df.rename(columns={'min': 'min_date', 'max': 'max_date'})

    # через левое соединение добавляем к ней нашу сгруппированную таблицу из новой статистики
    # таким образом мы видим по каким Продуктам НЕТ новой накопительной статистики
    one_target_views_df = one_target_views_df.merge(df, how='left', left_on=['source', 'product', 'report_type']
                                                                ,right_on=['source', 'product', 'report_type'])

    # к новой накопительной статистике через левое соединение добавляем данные из БД
    # теперь мы сможем понять
    # - для каких продуктов появились новые данные по досмотрам
    # - какое кол-во дней считаются новыми
    # - сможем расчитать кол-во досмотров по дням
    one_target_views_df = one_target_views_df.merge(db_video_views_df, how='left',
                                                             left_on=['source', 'product', 'report_type'],
                                                            right_on=['source', 'product', 'report_type'])
    
    one_target_views_df = one_target_views_df.fillna(0)
    
    # вызываем нашу функцию и записываем данные в новые поля
    one_target_views_df['min'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[0], axis=1)
    one_target_views_df['max'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[1], axis=1)
    one_target_views_df['i_25'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[2], axis=1)
    one_target_views_df['i_50'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[3], axis=1)
    one_target_views_df['i_75'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[4], axis=1)
    one_target_views_df['i_100'] = one_target_views_df.apply(lambda x: normalize_data_video_views(x)[5], axis=1)

    # создаем отдельный датаФрейм для расчета досмотров по дням
    # далее мы объединим его с основной таблицей статистики (добавим разбивку досмотров по дням для источника one target)
    # оставляем поля с нормализованными данными
    one_target_views_per_day = one_target_views_df[['source', 'product', 'report_type', 'start_period', 'end_period', 'min', 'max',
                                              'i_25', 'i_50', 'i_75', 'i_100']]
    
    # переименовываем некоторые поля
    one_target_views_per_day = one_target_views_per_day.rename(columns={'i_25': '25', 'i_50': '50', 'i_75': '75', 'i_100': '100'})
    one_target_views_per_day['count_days'] = one_target_views_per_day.apply(get_count_days, axis=1) # считаем кол-во дней между мин макс датой

    # считаем кол-во досмотров в день по каждому продукту
    one_target_views_per_day['25_per_day'] = one_target_views_per_day.apply(lambda x: get_views_by_day(x)[0], axis=1)
    one_target_views_per_day['50_per_day'] = one_target_views_per_day.apply(lambda x: get_views_by_day(x)[1], axis=1)
    one_target_views_per_day['75_per_day'] = one_target_views_per_day.apply(lambda x: get_views_by_day(x)[2], axis=1)
    one_target_views_per_day['100_per_day'] = one_target_views_per_day.apply(lambda x: get_views_by_day(x)[3], axis=1)

    # к таблице, в которой содержится статистика в разбивке по дням
    # через левое соединение по полю Продукт мы присоеднияем досмотры в день
    # таким образом унас получается  таблица, которая содержит полную статистику
    # теперь мы сможем объединить данные из всех источников в одну таблицу
    one_target_df = one_target_df.merge(one_target_views_per_day[['source', 'product', 'report_type', 
                                                             '25_per_day', '50_per_day', '75_per_day', '100_per_day']], how='left',\
                   left_on=['source', 'product', 'report_type'], 
                    right_on=['source', 'product', 'report_type'])
    
    # переименовываем поля, чтобы к ним было легче обращаться (меньше букв)
    one_target_df = one_target_df.rename(columns={'25_per_day': '25', '50_per_day': '50', '75_per_day': '75', '100_per_day': '100'})

    # датаФрейм, который содрежит накопительную статистику по досмотрам
    # оставляем только нужные поля
    one_target_views_df = one_target_views_df[['source', 'product', 'report_type', 'start_period', 'end_period', 
                                               'min', 'max', '25', '50', '75', '100']]

    one_target_views_df = one_target_views_df.rename(columns={'min': 'min_date', 'max': 'max_date'})
    
    return one_target_df, one_target_views_df


# In[ ]:





# In[ ]:




