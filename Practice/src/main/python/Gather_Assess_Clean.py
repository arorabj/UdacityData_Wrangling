#!/usr/bin/env python
# coding: utf-8

# ## Gather
import pandas as pd
import numpy as np

patients = pd.read_csv('patients.csv')
treatments = pd.read_csv('treatments.csv')
adverse_reactions = pd.read_csv('adverse_reactions.csv')

# ## Assess
patients
treatments
adverse_reactions
patients.info()
treatments.info()
adverse_reactions.info()
all_columns = pd.Series(list(patients) + list(treatments) + list(adverse_reactions))
all_columns[all_columns.duplicated()]
list(patients)
patients[patients['address'].isnull()]
patients.describe()
treatments.describe()
patients.sample(5)
patients.surname.value_counts()
patients.address.value_counts()
patients[patients.address.duplicated()]
patients.weight.sort_values()

weight_lbs = patients[patients.surname == 'Zaitseva'].weight * 2.20462
height_in = patients[patients.surname == 'Zaitseva'].height
bmi_check = 703 * weight_lbs / (height_in * height_in)
bmi_check
patients[patients.surname == 'Zaitseva'].bmi

sum(treatments.auralin.isnull())
sum(treatments.novodra.isnull())

# #### Quality
# ##### `patients` table
# - Zip code is a float not a string
# - Zip code has four digits sometimes
# - Tim Neudorf height is 27 in instead of 72 in
# - Full state names sometimes, abbreviations other times
# - Dsvid Gustafsson
# - Missing demographic information (address - contact columns) ***(can't clean)***
# - Erroneous datatypes (assigned sex, state, zip_code, and birthdate columns)
# - Multiple phone number formats
# - Default John Doe data
# - Multiple records for Jakobsen, Gersten, Taylor
# - kgs instead of lbs for Zaitseva weight
#
# ##### `treatments` table
# - Missing HbA1c changes
# - The letter 'u' in starting and ending doses for Auralin and Novodra
# - Lowercase given names and surnames
# - Missing records (280 instead of 350)
# - Erroneous datatypes (auralin and novodra columns)
# - Inaccurate HbA1c changes (leading 4s mistaken as 9s)
# - Nulls represented as dashes (-) in auralin and novodra columns
#
# ##### `adverse_reactions` table
# - Lowercase given names and surnames

# #### Tidiness
# - Contact column in `patients` table should be split into phone number and email
# - Three variables in two columns in `treatments` table (treatment, start dose and end dose)
# - Adverse reaction should be part of the `treatments` table
# - Given name and surname columns in `patients` table duplicated in `treatments` and `adverse_reactions` tables

# ## Clean

# In[23]:


patients_clean = patients.copy()
treatments_clean = treatments.copy()
adverse_reactions_clean = adverse_reactions.copy()

# ### Missing Data
# <font color='red'>Complete the following two "Missing Data" **Define, Code, and Test** sequences after watching the *"Address Missing Data First"* video.</font>
# #### `treatments`: Missing records (280 instead of 350)
# ##### Define
# *Your definition here. Note: the missing `treatments` records are stored in a file named `treatments_cut.csv`, which you can see in this Jupyter Notebook's dashboard (click the **jupyter** logo in the top lefthand corner of this Notebook). Hint: [documentation page](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.concat.html) for the function used in the solution.*
# ##### Code

# Your cleaning code here
df_treadtments_cut = pd.read_csv('treatments_cut.csv')
treatments_clean = pd.concat([treatments_clean, df_treadtments_cut], ignore_index=True)

# ##### Test
# Your testing code here
treatments_clean.head()
treatments_clean.tail()

# #### `treatments`: Missing HbA1c changes and Inaccurate HbA1c changes (leading 4s mistaken as 9s)
# *Note: the "Inaccurate HbA1c changes (leading 4s mistaken as 9s)" observation, which is an accuracy issue and not a completeness issue, is included in this header because it is also fixed by the cleaning operation that fixes the missing "Missing HbA1c changes" observation. Multiple observations in one **Define, Code, and Test** header occurs multiple times in this notebook.*
# ##### Define
# *Your definition here.*
# ##### Code

# Your cleaning code here
treatments_clean.hba1c_change = treatments_clean.hba1c_start - treatments_clean.hba1c_end
treatments_clean.tail()

# ##### Test
# Your testing code here
treatments_clean.tail()

# ### Tidiness
# <font color='red'>Complete the following four "Tidiness" **Define, Code, and Test** sequences after watching the *"Cleaning for Tidiness"* video.</font>
# #### Contact column in `patients` table contains two variables: phone number and email
# ##### Define
# *Your definition here. Hint 1: use regular expressions with pandas' [`str.extract` method](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.str.extract.html). Here is an amazing [regex tutorial](https://regexone.com/). Hint 2: [various phone number regex patterns](https://stackoverflow.com/questions/16699007/regular-expression-to-match-standard-10-digit-phone-number). Hint 3: [email address regex pattern](http://emailregex.com/), which you might need to modify to distinguish the email from the phone number.*
# ##### Code
# Your cleaning code here
patients_clean['phone'] = patients_clean.contact.str.extract('((?:\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})',
                                                             expand=True)
patients_clean['email'] = patients_clean.contact.str.extract(
    '([a-zA-Z][a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+[a-zA-Z])', expand=True)
patients_clean = patients_clean.drop('contact', axis=1)

# ##### Test
# Your testing code here
patients_clean.head()

# #### Three variables in two columns in `treatments` table (treatment, start dose and end dose)
# ##### Define
# *Your definition here. Hint: use pandas' [melt function](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.melt.html) and [`str.split()` method](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.str.split.html). Here is an excellent [`melt` tutorial](https://deparkes.co.uk/2016/10/28/reshape-pandas-data-with-melt/).*
# ##### Code

# Your cleaning code here
treatments_clean = pd.melt(treatments_clean,
                           id_vars=['given_name', 'surname', 'hba1c_start', 'hba1c_end', 'hba1c_change'],
                           var_name='treatment', value_name='dose')
treatments_clean = treatments_clean[treatments_clean.dose != "-"]
treatments_clean['dose_start'], treatments_clean['dose_end'] = treatments_clean['dose'].str.split(' - ', 1).str

treatments_clean = treatments_clean.drop('dose', axis=1)

# ##### Test
# Your testing code here
treatments_clean.head()

# #### Adverse reaction should be part of the `treatments` table
# ##### Define
# *Your definition here. Hint: [tutorial](https://chrisalbon.com/python/pandas_join_merge_dataframe.html) for the function used in the solution.*
# ##### Code
# Your cleaning code here
treatments_clean = pd.merge(treatments_clean, adverse_reactions_clean, on=['given_name', 'surname'], how='left')

# ##### Test
# Your testing code here
treatments_clean

# #### Given name and surname columns in `patients` table duplicated in `treatments` and `adverse_reactions` tables  and Lowercase given names and surnames
# ##### Define
# *Your definition here. Hint: [tutorial](https://chrisalbon.com/python/pandas_join_merge_dataframe.html) for one function used in the solution and [tutorial](http://erikrood.com/Python_References/dropping_rows_cols_pandas.html) for another function used in the solution.*
# ##### Code

# Your cleaning code here
id_names = patients_clean[['patient_id', 'given_name', 'surname']]
id_names.given_name = id_names.given_name.str.lower()
id_names.surname = id_names.surname.str.lower()
treatments_clean = pd.merge(treatments_clean, id_names, on=['given_name', 'surname'])
treatments_clean = treatments_clean.drop(['given_name', 'surname'], axis=1)

# ##### Test
# Your testing code here
treatments_clean
all_columns = pd.Series(list(treatments_clean) + list(patients_clean))
all_columns[all_columns.duplicated()]

# ### Quality
# <font color='red'>Complete the remaining "Quality" **Define, Code, and Test** sequences after watching the *"Cleaning for Quality"* video.</font>
# #### Zip code is a float not a string and Zip code has four digits sometimes

# ##### Define
# *Your definition here. Hint: see the "Data Cleaning Process" page.*
# ##### Code

# Your cleaning code here
patients_clean.zip_code = patients_clean.zip_code.astype(str).str[:-2].str.pad(5, fillchar='0')
patients_clean.zip_code = patients_clean.zip_code.replace('0000n', np.nan)

# ##### Test
# Your testing code here
patients_clean.zip_code

# #### Tim Neudorf height is 27 in instead of 72 in
# ##### Define
# *Your definition here.*
# ##### Code

# Your cleaning code here
patients_clean.height = patients_clean.height.replace(27, 72)

# ##### Test

# Your testing code here
patients_clean[patients_clean.height == 27]
patients_clean[patients_clean.surname == 'Neudorf']

# #### Full state names sometimes, abbreviations other times

# ##### Define
# *Your definition here. Hint: [tutorial](https://chrisalbon.com/python/pandas_apply_operations_to_dataframes.html) for method used in solution.*
# ##### Code

# Your cleaning code here
state_abb = {'California': 'CA', 'New York': 'NY', 'Illinois': 'IL', 'Florida': 'FL', 'Nebraska': 'NE'}


def replace_state_abb(patient):
    if patient['state'] in state_abb.keys():
        abb = state_abb[patient['state']]
        return abb
    else:
        return patient['state']


patients_clean['state'] = patients_clean.apply(replace_state_abb, axis=1)

# ##### Test
# Your testing code here
patients_clean.state.value_counts()

# #### Dsvid Gustafsson
# ##### Define
# *Your definition here.*
# ##### Code
# Your cleaning code here
patients_clean.given_name = patients_clean.given_name.replace('Dsvid', 'David')

# ##### Test
# Your testing code here
patients_clean[patients_clean.surname == 'Gustafsson']

# #### Erroneous datatypes (assigned sex, state, zip_code, and birthdate columns) and Erroneous datatypes (auralin and novodra columns) and The letter 'u' in starting and ending doses for Auralin and Novodra
# ##### Define
# *Your definition here. Hint: [documentation page](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.astype.html) for one method used in solution, [documentation page](http://pandas.pydata.org/pandas-docs/version/0.20/generated/pandas.to_datetime.html) for one function used in the solution, and [documentation page](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.str.strip.html) for another method used in the solution.*
# ##### Code

# Your cleaning code here
patients_clean.assigned_sex = patients_clean.assigned_sex.astype('category')
patients_clean.state = patients_clean.state.astype('category')

patients_clean.birthdate = pd.to_datetime(patients_clean.birthdate)

treatments_clean.dose_start = treatments_clean.dose_start.str.strip('u').astype(int)
treatments_clean.dose_end = treatments_clean.dose_end.str.strip('u').astype(int)

# ##### Test
# Your testing code here
patients_clean.info()
treatments_clean.info()

# #### Multiple phone number formats
# ##### Define
# *Your definition here. Hint: helpful [Stack Overflow answer](https://stackoverflow.com/a/123681).*
# ##### Code

# Your cleaning code here
patients_clean.phone = patients_clean.phone.str.replace(r'\D+', '').str.pad(11, fillchar='1')
# ##### Test

# Your testing code here
patients_clean.phone

# #### Default John Doe data
# ##### Define
# *Your definition here. Recall that it is assumed that the data that this John Doe data displaced is not recoverable.*
# ##### Code

# Your cleaning code here
patients_clean = patients_clean[patients_clean.surname != 'Doe']

# ##### Test
# Your testing code here
patients_clean.surname.value_counts()

# #### Multiple records for Jakobsen, Gersten, Taylor
# ##### Define
# *Your definition here.*
# ##### Code
# Your cleaning code here
patients_clean = patients_clean[~(patients_clean.address.duplicated() & patients_clean.address.notnull())]
# ##### Test
# Your testing code here
patients_clean[patients_clean.surname == 'Jakobsen']
patients_clean[patients_clean.surname == 'Gersten']
# #### kgs instead of lbs for Zaitseva weight
patients_clean[patients_clean.surname == 'Taylor']

# ##### Define
# *Your definition here.*
# ##### Code
# Your cleaning code here
wt_kg = patients_clean.weight.min()
mask = patients_clean.surname == 'Zaitseva'
column_name = 'weight'
patients_clean.loc[mask, column_name] = wt_kg * 2.20462

# ##### Test
# Your testing code here
patients_clean[patients_clean.surname == 'Zaitseva']





