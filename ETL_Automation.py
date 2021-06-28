#!/usr/bin/env python
# coding: utf-8

# # ETL Automation

# ## Import libraries

# In[1]:


import gspread
import gspread_dataframe as gd
import pandas as pd
import os
import datetime


# In[76]:


# os.chdir('/Users/atmavidyavirananda/Desktop/Tableau Public Project')
# os.rename("/Users/atmavidyavirananda/Desktop/ETL Automation.ipynb", "/Users/atmavidyavirananda/Desktop/Tableau Public Project/ETL Automation.ipynb")


# ## Import data from Github

# In[8]:


# Path to Github repository, in which the COVID-19 data is updated daily
main_dir = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'

confirmed_path = 'time_series_covid19_confirmed_global.csv'
deaths_path = 'time_series_covid19_deaths_global.csv'
recovered_path = 'time_series_covid19_recovered_global.csv'

paths = [confirmed_path, deaths_path, recovered_path]


# In[9]:


df_names = ['confirmed_global', 'deaths_global', 'recovered_global']
data = {}

# Get data, create dataframe, then store in a dictionary.
for p,name in zip(paths,df_names):
    data[name] = pd.read_csv(os.path.join(main_dir,p))
    print(f'Extracted {name} data.')
    


# In[10]:


# # Preview data
# print(data['confirmed_global'].shape)
# data['confirmed_global'].head()


# ## ASEAN Countries

# In[11]:


# Manually created list to search for ASEAN countries in the Country/Region column
# Source: https://asean.org/asean/asean-member-states/
asean_list = ['brunei', 'cambodia', 'indonesia', 'lao', 'malaysia', 'myanmar', 'philippines', 'singapore', 'thailand', 'vietnam']


# In[12]:


# Empty list to store the actual country name from the Country/Region column
asean_countries = []


# In[13]:


# This function will be executed for each value in the Country/Region column
def find_asean(val):
    for country in asean_list:
        if country in val.lower(): # if the current column value matches any of the countries in asean_list
            asean_countries.append(val) # append that value to the asean_countries list


# In[14]:


# Execute function with a map method
data['confirmed_global']['Country/Region'].map(find_asean)


# In[15]:


# # List of asean country names that is consistent with the names in the original data (Country/Region column)
# asean_countries


# In[16]:


# Create a dataframe for confirmed cases, deaths, and recovered that has been filtered for ASEAN countries
asean_confirmed = data['confirmed_global'].loc[data['confirmed_global']['Country/Region'].isin(asean_countries),:]
asean_deaths = data['deaths_global'].loc[data['deaths_global']['Country/Region'].isin(asean_countries),:]
asean_recovered = data['recovered_global'].loc[data['recovered_global']['Country/Region'].isin(asean_countries),:]


# In[17]:


# Drop the Province/State since they are missing for every ASEAN country
for df in [asean_confirmed, asean_deaths, asean_recovered]:
    df.drop('Province/State', inplace=True, axis=1)


# ## Transform

# If we look at the raw data, we notice that the time series extends horizontally along the table. In other words, the dates are arranged along the columns instead of the rows, which would make it harder to create out visualizations in Tableau. This part is intended to illustrate how we would wrangle the data such that the time series are presented vertically in one column for every country.

# In[18]:


# declare parameters for melting the dataframe
dim_cols = data['confirmed_global'].columns[1:4]
ts_cols = data['confirmed_global'].columns[4:]


# In[19]:


metrics = ['Confirmed', 'Deaths', 'Recovered'] 
rsh = {} # dictionary of reshaped dataframes

# Loop through each dataframe and transform, then store in the designated dictionary.
for df,m in zip([asean_confirmed, asean_deaths, asean_recovered], metrics):
    rsh[m] = df.melt(id_vars=dim_cols, value_vars=ts_cols, var_name='Date', value_name = m)


# ### Combine the 3 data sources into 1 dataframe

# In[20]:


all_data = rsh['Confirmed'].sort_values(by=['Country/Region','Confirmed']).copy()


# In[21]:


all_data['Deaths'] = rsh['Deaths'].sort_values(by=['Country/Region','Deaths'])['Deaths']
all_data['Recovered'] = rsh['Recovered'].sort_values(by=['Country/Region','Recovered'])['Recovered']


# In[25]:


all_data['Date'] = pd.to_datetime(all_data['Date'])


# In[33]:


all_data.rename(columns={'Country/Region':'Country'}, inplace=True)


# In[158]:


# country_loc = all_data.groupby('Country/Region').max()[['Lat','Long']].reset_index()


# ## Export to Google Sheets

# In[2]:


# Use Google Sheet API credentials to authorize access 
gc = gspread.service_account('/Users/atmavidyavirananda/Desktop/tableau-public-viz-3d037cec5382.json')


# In[3]:


# Access the premade sheet (make sure the generated credential is given editor access to the sheet)
wks = gc.open('ASEAN COVID-19 Data - Daily')
ts_data = wks.worksheet('Time series')
latest_data = wks.worksheet('Latest')
log = wks.worksheet('Log')


# In[36]:


# Write time series data
ts_data.clear()
gd.set_with_dataframe(ts_data, all_data)


# In[37]:


# Write latest data
max_date = all_data['Date'].max()
latest = all_data.loc[all_data['Date']==max_date,:]

latest_data.clear()
gd.set_with_dataframe(latest_data, latest)


# In[38]:


# Update log
curr_timestamp = datetime.datetime.now().strftime('%D %H:%M:%S')
log.update('A1', curr_timestamp)

