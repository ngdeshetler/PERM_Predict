# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 08:02:04 2016

@author: Natalie
"""
import os
import sqlalchemy as sql
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd

os.chdir('/Users/Natalie/Documents/Insight/PERM_excel')

keys_dic={}

def names_fix(column_names):
    cnames={}
    for names in column_names:
        cnames[names]=names.upper().replace(" ","_").replace("JI_","JOB_INFO_").replace("EMP_","EMPLOYER_").replace("ATT_","AGENT_").replace("PW_","PERVAILING_WAGE_").replace("CITZENSHIP","CITIZENSHIP").replace("ATTORNEY_","AGENT_").replace("_9089","")
    return cnames

years=['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','20014','2015','2016']

for year in years:
    dff=pd.DataFrame.from_csv('PERM_FY'+year+'.csv')
    dff.rename(columns=names_fix(dff.columns.values),inplace=True)
    keys_dic[year]=dff


PERM_data=pd.concat(keys_dic.values())
PERM_data.columns.values

dbname = 'PERM_db'
username = 'Natalie'
pswd = 'green,earth88'

engine = sql.create_engine('postgresql://%s:%s@localhost/%s'%(username,pswd,dbname))
print engine.url

if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))

PERM_data.to_sql('PERM_data_table', engine, if_exists='replace')