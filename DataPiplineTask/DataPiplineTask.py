#!/usr/bin/env python
# coding: utf-8

# In[5]:


#!pip install psycopg2
import psycopg2
from psycopg2 import *


# In[6]:


from pprint import pprint
import vk_api
import re
import sys

import logging as log
import pandas as pd
import matplotlib.pyplot as plt
import vk_api

# remove symbols: " ) ( # & << » [ ] «
# remove words with: digits / length==0or 1 club
removingChars = ["(", ")", "[", "]", "«", "»", "&", "!", "\""]
banningSubs = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "/", "club", "\\", ";", "#", "@"]

s = ["efw"]
s

def remove_char(input_word, char):
    resultWord = input_word.replace(char, "")
    return resultWord


def clear_word(word_clear):  # Убираем ненужные символы
    for char in removingChars:
        word_clear = remove_char(word_clear, char)
    return word_clear


def filter_word(filtering_word):  # Удаляем неподходящие слова
    if len(filtering_word) == 1 or len(filtering_word) == 0:
        return False
    for sub in banningSubs:
        if sub in filtering_word:
            return False
    return True


def get_words(text):  # Рaзбиваем на слова данные
    text = text.replace("\n", " ")
    text = text.replace(",", "").replace(".", "").replace("?", "").replace("!", "")
    text = text.lower()
    words = text.split()
    words.sort()
    return words


def get_words_dict(words):  # Подсчет слов
    words_filtered = []
    clear_words = []
    words_dictionary = dict()

    for word_item in words:
        if filter_word(word_item):
            words_filtered.append(word_item)

    for word_item in words_filtered:
        clear_words.append(clear_word(word_item))

    for word_item in clear_words:
        if word_item in words_dictionary:
            words_dictionary[word_item] = words_dictionary[word_item] + 1
        else:
            words_dictionary[word_item] = 1
    return words_dictionary


vk_session = vk_api.VkApi("+79053749485", "QaWsEd16")  # Проводим авторизацию(логин и пароль от вк)
vk_session.auth()
id = 'id641678009'  # Вводим свой айдишник
api = vk_session.get_api()
count_posts = int(sys.argv[1])
target_domain = sys.argv[2]  # -itis_kfu
db_endpoint = sys.argv[3] # example mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com
db_user = sys.argv[4]
db_password =sys.argv[5]
db_name = sys.argv[6]
db_port = sys.argv[7]   # postgresql+psycopg2://postgres:qwerty016@mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com:5432/postgres
groups = api.groups.get()
log.info(count_posts)
log.info(target_domain)
print(count_posts)
print(target_domain)
print(db_endpoint)
pprint(groups)
coll_walls = []
# walls = api.wall.get(domain='itis_kfu', count=100)  # Получили все посты
# walls2 = api.wall.get(domain='itis_kfu', count=100,offset=100)

for i in range(0,count_posts,100):
    walls = api.wall.get(domain=target_domain, count=100,offset=i)
    coll_walls.append(walls)


resultText = ""


# In[ ]:





# In[ ]:


# todo: Сделать цикл для любого кол-ва постов
for wall in coll_walls:
    for item in wall['items']:
        if('attachments' in item):
            for video in item['attachments']:
                if video['type'] == 'video':
                    resultText +=video['video']['title']
                    resultText +=video['video']['description']
        resultText = resultText + item['text']
        if ('title' in item):
            resultText += item['title']

words_dict = get_words_dict(get_words(resultText))  # Итоговый словарь (слово - кол-во вхождений)

words_dict = dict(sorted(words_dict.items(), key=lambda dict_item: dict_item[1]))

#df = pd.DataFrame.from_dict(words_dict, orient='index').transpose()



# logs
for word in words_dict:
    print(word.ljust(20), words_dict[word])


# In[ ]:


plt.rcParams["figure.figsize"]=15,10


# In[ ]:


plt.title('Words statistics')

words_top = 100

bars = list(words_dict.values())[len(words_dict)-words_top:]
xticks = list(words_dict.keys())[len(words_dict)-words_top:]

plt.bar(range(words_top), bars, align='center')
plt.xticks(range(words_top), xticks, rotation='vertical')
plt.tight_layout()
#fig=plt.figure(figsize=(22, 18), dpi= 400, facecolor='w', edgecolor='k')
plt.savefig('words-top100.png')
# plt.show()




# In[8]:


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


# In[9]:


connection = create_connection(
    db_name, db_user, db_password, db_endpoint, db_port
)


# In[8]:


create_itis_words_query ="""
CREATE TABLE IF NOT EXISTS vk_itis_words (
  word TEXT  PRIMARY KEY ,
  count bigint
  
);
"""
create_database(connection, create_itis_words_query)


# In[9]:


mass = []
for word in words_dict:
    mass.append((word.ljust(20).strip(), words_dict[word]))
    
words_records = ", ".join(["%s"] * len(mass))


# In[10]:


insert_query = (
    f"INSERT INTO vk_itis_words  (word,count) VALUES {words_records}"
)

connection.autocommit = True
cursor = connection.cursor()
cursor.execute(insert_query, mass)


# In[4]:





# In[ ]:




