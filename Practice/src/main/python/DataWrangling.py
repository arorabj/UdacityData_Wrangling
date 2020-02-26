import zipfile

import matplotlib as matplotlib
import pandas as pd
import numpy as np

wd='/Users/anantarora/Downloads/udacity/'
with zipfile.ZipFile(wd+'armenian-online-job-postings.zip','r') as myzip:
    print(1)
    myzip.extractall(path=wd)

df = pd.read_csv(wd+'online-job-postings.csv')
#print(df.Year.value_counts())
df_clean = df.copy()
df_clean.rename(columns={'jobpost':'JobPost',
                         'date':'Date',
                         'JobRequirment':'JobRequirement',
                         'RequiredQual':'RequiredQualification',
                         'AboutC':'AboutCompany',
                         'ApplicationP':'ApplicationProcedure'
                         },inplace=True)

asap_list = ['Immediately', 'As soon as possible', 'Upon hiring',
             'Immediate', 'Immediate employment', 'As soon as possible.', 'Immediate job opportunity',
             '"Immediate employment, after passing the interview."',
             'ASAP preferred', 'Employment contract signature date',
             'Immediate employment opportunity', 'Immidiately', 'ASA',
             'Asap', '"The position is open immediately but has a flexible start date depending on the candidates earliest availability."',
             'Immediately upon agreement', '20 November 2014 or ASAP',
             'immediately', 'Immediatelly',
             '"Immediately upon selection or no later than November 15, 2009."',
             'Immediate job opening', 'Immediate hiring', 'Upon selection',
             'As soon as practical', 'Immadiate', 'As soon as posible',
             'Immediately with 2 months probation period',
             '12 November 2012 or ASAP', 'Immediate employment after passing the interview',
             'Immediately/ upon agreement', '01 September 2014 or ASAP',
             'Immediately or as per agreement', 'as soon as possible',
             'As soon as Possible', 'in the nearest future', 'immediate',
             '01 April 2014 or ASAP', 'Immidiatly', 'Urgent',
             'Immediate or earliest possible', 'Immediate hire',
             'Earliest  possible', 'ASAP with 3 months probation period.',
             'Immediate employment opportunity.', 'Immediate employment.',
             'Immidietly', 'Imminent', 'September 2014 or ASAP', 'Imediately']

for i in asap_list:
    df_clean.StartDate.replace(i,'ASAP', inplace=True)
    #testing if the phrase is removed or not
    assert i not in df_clean.StartDate.values

percentage_df_clean = df_clean.StartDate.value_counts()['ASAP']/df_clean.StartDate.count()
print(percentage_df_clean)

percentage_df = df.StartDate.value_counts()['ASAP']/df.StartDate.count()
print(percentage_df)




