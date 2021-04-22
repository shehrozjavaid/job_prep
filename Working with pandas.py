# -*- coding: utf-8 -*-
"""
Created on Tue Apr 20 20:55:54 2021

@author: Shehroz Javaid
"""

import pandas as pd
import json, sys

from psycopg2 import connect, Error
from sqlalchemy import create_engine

names = ['Id','Item Name','Customer Name','Quantity','M1','M2','M3','Loation','Proeduct Category','Discount']
df = pd.read_csv('Sample-Spreadsheet-1000-rows.csv',names=names,encoding = "ISO-8859-1", engine='python')
df['is_done']=pd.Series(True, index=df.index)
df.to_json("output.json")

########

df = pd.read_json("output.json")

try:
    
    connection = connect(user="postgres",
                                  password="5555",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="test_db")


    cursor = connection.cursor()
    engine = create_engine('postgresql://postgres:5555@localhost:5432/test_db')
    df.to_sql('test_table',engine)
    records = engine.execute('SELECT * FROM test_table').fetchall()    
    for i in records:
        print(i)
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
        
        
        
        
        
        