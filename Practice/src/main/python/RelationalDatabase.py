import sqlalchemy
import sqlalchemy_utils
import pandas as pd
import os

folder= '/Users/anantarora/Downloads/udacity/'

df = pd.read_csv(os.path.join(folder,'bestofrt.tsv'),sep='\t')
if sqlalchemy_utils.database_exists('sqlite:///bestofrt.db'):
    sqlalchemy_utils.drop_database('sqlite:///bestofrt.db')

db = sqlalchemy.create_engine('sqlite:///bestofrt.db')
df.to_sql('master',db, index=False)

df_read =pd.read_sql('select * from master',db)


print(df_read.head(3))






