from bs4 import BeautifulSoup
import pandas as pd
import os

folder = '/Users/anantarora/Downloads/udacity/rt_html/'
dirlist = os.listdir(folder)
df_list =list()

for files in dirlist:
    with open(os.path.join(folder,files)) as myfile:
        soup = BeautifulSoup(myfile,features="html.parser")
        title = soup.find('title').contents[0][:-len(' - Rotten Tomatoes')]
        audience_score =soup.find('div',class_='audience-score meter').find('span').contents[0][:-1]
        num_audience_ratings = soup.find('div',class_='audience-info hidden-xs superPageFontColor').find_all('div')[1].contents[2].strip().replace(',','')

        # print (title,audience_score,num_audience_ratings)
        df_list.append({'title': title,
                            'audience_score': int(audience_score),
                            'number_of_audience_ratings': int(num_audience_ratings)})
df = pd.DataFrame(df_list, columns=['title','audience_score','number_of_audience_ratings'])
print (df)




