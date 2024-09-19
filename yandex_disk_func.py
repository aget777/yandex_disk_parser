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
import gc
from pandas.api.types import is_string_dtype
import numpy as np
import warnings


# УРЛ для операций над опубликованными папками и файлами на Яндекс Диске
base_public_url = 'https://cloud-api.yandex.net/v1/disk/public/resources?'  

download_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?' # УРЛ для скачивания эксель файлов из Яндекс Диска
delete_url = 'https://cloud-api.yandex.net/v1/disk/resources?' # УРЛ для удаления папок и файлов на Яндекс Диске


# In[ ]:


def get_yandex_disk_folders(public_key):
    
    final_url = base_public_url + urlencode(dict(public_key=public_key))  # Формируем УРЛ для получения списка папок и файлов
    res = requests.get(final_url)  # Отправлем запрос на Яндекс диск, чтобы получить название папок и public key для скачивания файлов
    
    print(res.status_code)
    
    return res # парсим ответ


# In[ ]:


# Создаем функцию для получения содержимого Яндекс диска
#  на входе принимаем 
# - адрес запроса для скачивания файла
# - public key, который мы получаем из ответа Яндекс при запросе информации по конкретной папке
# - адрес папки
# - лимит файлов (максимальное кол-во 80)
def get_yandex_disk_responce(request_url, public_key, folder_path, limit=80):
    res_url = request_url + urlencode(dict(public_key=public_key, path=folder_path, limit=limit)) # формируем строку запроса
    res = requests.get(res_url) # отправляем запрос на сервер
    yandex_responce = res.json() # получаем ответ и преобразуем его в json

    return yandex_responce


# In[ ]:


# создаем функцию для удаления файлов после загрузки из Яндекс Диска
# мы удаляем только те файлы, которые были загружены
# как пример - Если название файла не соответствует согласованному, то такой файл НЕ будет загружен в БД
# соответсвенно он НЕ будет удален из папки
# и по итогу мы увидим, какие новые НЕ согласованные файлы остались НЕ загруженными

def delete_yandex_disk_file(main_folder, file_path, yandex_token):
    # final_delete_url = '' # создаем пустую строковую переменную для формирования пути удаления файлов
    url_path = urlencode(dict(path=main_folder+file_path)) # кодируем полный путь к файлу вместе с его названием
    
    # добавляем флаг permanently=True для полного удаления файлов
    final_delete_url = delete_url + url_path + '&permanently=True'
    # формируем заголовки для дальнейших запросов
    headers = {
        'Content-Type': 'application/json', 
        'Accept': 'application/json', 
        'Authorization': f'OAuth {yandex_token}'
    }
    
    requests.delete(final_delete_url, headers=headers) #вызываем метод delete для удаления


# In[ ]:





# In[ ]:




