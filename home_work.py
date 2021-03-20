# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import datetime

flights=pd.read_csv('C:/Users/ravil/Desktop/flights.csv', parse_dates=['date'])
hotels=pd.read_csv('C:/Users/ravil/Desktop/hotels.csv', parse_dates=['date'])
users=pd.read_csv('C:/Users/ravil/Desktop/users.csv')

start_date = datetime.datetime(2020, 4, 1)
flights=flights[flights['date'] >= start_date]

users=users[(users.age>35) & (users.gender=='female')]

df=flights.merge(users, how='inner', left_on='userCode', right_on='code')
del df['code']
del df['userCode']
df = df.rename(columns={'name': 'user_name'})

df=df.merge(hotels, how='inner', suffixes=('_flight', '_hotel'), left_on='travelCode', right_on='travelCode')
del df['userCode']
del df['travelCode']

df.to_parquet('C:/Users/ravil/Desktop/HW/df.parquet.gzip', compression='gzip')