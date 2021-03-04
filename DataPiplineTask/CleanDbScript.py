#!/usr/bin/env python
# coding: utf-8

# In[1]:
import sys

import psycopg2
from psycopg2 import *


# In[ ]:


db_endpoint = sys.argv[1] # example mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com
db_user = sys.argv[2]
db_password =sys.argv[3]
db_name = sys.argv[4]
db_port = sys.argv[5]   # postgresql+psycopg2://postgres:qwerty016@mydatabase.cdwb7v1ldkf1.us-east-1.rds.amazonaws.com:5432/postgres


# In[2]:


def create_connection(db_name, db_user, db_password, db_endpoint, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_endpoint,
            port=db_port,
        )
        return  connection
        print("Connection to PostgreSQL DB successful")
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


# In[ ]:


connection = create_connection(
    db_name, db_user, db_password, db_endpoint, db_port
)


# In[ ]:
delete_comment = "TRUNCATE  vk_itis_words"
execute_query(connection, delete_comment)


# In[ ]:




