import glob
import pandas as pd

folder= '/Users/anantarora/Downloads/udacity/ebert_reviews'

df_list = list()

for myfiles in glob.glob(folder+'/*.txt'):
    with open(myfiles,encoding='utf-8') as file:
        line = file.readlines()
        title = line[0].strip('\n')
        review_url = line[1].strip('\n')
        review_text = line[2].strip('\n')

        df_list.append({'title':title,'review_url':review_url,'review_text':review_text})

df = pd.DataFrame(df_list,columns =['title','review_url','review_text'])
print (df)

