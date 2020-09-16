#!/usr/bin/env python
# coding: utf-8

# # Extract Covid-19 data from website grainmart.in using BeautifulSoup

# In[1]:


# importing the libraries
from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd


# In[2]:


covid_source_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/covid_data_unformatted.csv"
covid_target_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/covid_data.csv"
gsdp_source_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/gsdp_state_wise_unformatted.csv"
gsdp_target_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/gsdp_state_wise.csv"
unemployment_source_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/unemployment_unformatted.csv"
unemployment_target_filename = '/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/unemployment.csv'
population_source_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/population_unformatted.csv"
population_target_filename = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/population.csv"
covid_pop_uemp_gsdp_output_file_name = "/home/sanjay/campaign/dev_codes/jupyter_lab/covid_19_hackathon/dataset/final_data_test_1.csv"


# In[3]:


url="https://www.grainmart.in/news/covid-19-coronavirus-india-state-and-district-wise-tally"

# Make a GET request to fetch the raw HTML content
html_content = requests.get(url).text

# Parse the html content
#soup = BeautifulSoup(html_content, "lxml")
soup = BeautifulSoup(html_content,"html.parser")
# print(soup.prettify()) # print the parsed data of html


# In[4]:


section_data = soup.find("section", {"id": "covid-19-table"})
main_list = []


# In[5]:


states_rows = section_data.find_all("div", {"class": "skgm-states"})


# In[6]:


def clean_up_soup(pre_content):
    pre_content.select("div.td-ddd.td-ddr.td-ddc")
    for tags in pre_content.findAll(True): 
        tags.attrs = {}
    return pre_content


# In[7]:



final_state_list = []
fianle_district_list = []
headers_list = [
    'state_name',
    'cases',
    'cured',
    'active',
    'deaths',
    'district_name',
    'district_cases',
    'district_cured',
    'district_active',
    'district_deaths'
]
for dat in states_rows:
    state_list = []
    state_name = dat.find_all("span", {"class": "show-district"})[0].contents[-1]
    cases = int(dat.find_all("div", {"class": "td-sc"})[0].contents[0])
    cured = int(dat.find_all("div", {"class": "td-sr"})[0].contents[0])
    active = int(dat.find_all("div", {"class": "td-sa"})[0].contents[0])
    deaths = int(dat.find_all("div", {"class": "td-sd"})[0].contents[0])
    state_list.append(state_name)
    state_list.append(cases)
    state_list.append(cured)
    state_list.append(active)
    state_list.append(deaths)
    final_state_list.append(state_list)
    inner_main = dat.find_all("div", {"class": "skgm-tr"})
    index = 0
    for inner_list in inner_main:
        if index == 0:
            index = index + 1
            continue
        inner_list = clean_up_soup(inner_list)
        childs = list(inner_list.children)
        district_names = childs[0].contents[0]
        district_cases = childs[1].contents[0].contents[0]
        district_cured = childs[2].contents[0].contents[0]
        district_active = childs[3].contents[0].contents[0]
        district_deaths = childs[4].contents[0].contents[0]
        each_district_list = [
            state_name,
            cases,
            cured,
            active,
            deaths,
            district_names,
            district_cases,
            district_cured,
            district_active,
            district_deaths
        ]
        fianle_district_list.append(each_district_list)
    


# In[8]:


# writing to csv file  
with open(covid_source_filename, 'w') as csvfile:  
    # creating a csv writer object  
    csvwriter = csv.writer(csvfile)  
        
    # writing the fields  
    csvwriter.writerow(headers_list)  
        
    # writing the data rows  
    csvwriter.writerows(fianle_district_list)


# # Transform GSDP dataset as per requirements using Pandas

# In[33]:


# reading csv file  
gsdp_df = pd.read_csv(gsdp_source_filename)
gsdp_df.rename(
    columns={
        'year':'gsdp_year'
    },
    inplace=True
)


# In[34]:


gsdp_dataset = gsdp_df.melt(id_vars=["gsdp_year"], var_name="gsdp_region", value_name="gsdp_in_crores")
gsdp_dataset['gsdp_year'] = pd.to_datetime(gsdp_dataset['gsdp_year'].astype(str), format='%Y')
gsdp_dataset['gsdp_year'] = pd.to_datetime(gsdp_dataset['gsdp_year']).dt.to_period('Y')


# In[35]:


gsdp_dataset= gsdp_dataset.applymap(lambda s:s.lower() if type(s) == str else s)


# In[36]:


gsdp_dataset.to_csv(gsdp_target_filename, sep=',', encoding='utf-8', index = False)


# # Unemployment dataset
# ### source - unemploymentinindia.cmie.com

# In[37]:


# reading csv file  
unemployment_df = pd.read_csv(unemployment_source_filename)
unemployment_df= unemployment_df.applymap(lambda s:s.lower() if type(s) == str else s)


# In[38]:


unemployment_df.rename(
    columns={
        'Estimated Unemployment Rate (%)':'unemployment_rate',
        'Region':'unemployment_region',
        'Date': 'date',
        'Estimated Employed': 'employed',
        'Estimated Labour Participation Rate (%)': 'lbour_participation_rate'
    },
    inplace=True
)


# In[39]:


unemployment_df['unemployment_year'] = pd.to_datetime(unemployment_df['date']).dt.to_period('Y')
unemployment_grouped_df = unemployment_df.groupby(['unemployment_region', 'unemployment_year']).agg(
    {'unemployment_rate': ['mean', 'min', 'max'],
     'employed': ['mean', 'min', 'max'],
     'lbour_participation_rate': ['mean', 'min', 'max']})


# In[40]:


unemployment_grouped_df.columns = ["_".join(x) for x in unemployment_grouped_df.columns.ravel()]


# In[41]:


unemployment_grouped_df['unemployment_rate_mean'] = unemployment_grouped_df['unemployment_rate_mean'].round(2) 
unemployment_grouped_df['employed_mean'] = unemployment_grouped_df['employed_mean'].astype(int)
unemployment_grouped_df['lbour_participation_rate_mean'] = unemployment_grouped_df['lbour_participation_rate_mean'].round(2) 


# In[42]:


unemployment_grouped_df.reset_index(inplace=True)
unemployment_grouped_df.to_csv(unemployment_target_filename, sep=',', encoding='utf-8', index = False)


# # Population Dataset
# ### Source - PDF - https://nhm.gov.in/New_Updates_2018/Report_Population_Projection_2019.pdf

# In[85]:


# reading csv file  
population_df = pd.read_csv(population_source_filename)
population_df.rename(
    columns={
        'Year':'population_year'
    },
    inplace=True
)


# In[86]:


population_dataset = population_df.melt(
    id_vars=["population_year"], var_name="population_region", value_name="population"
)
population_dataset['population_year'] = pd.to_datetime(population_dataset['population_year'].astype(str), format='%Y')
population_dataset['population_year'] = pd.to_datetime(population_dataset['population_year']).dt.to_period('Y')
population_dataset= population_dataset.applymap(lambda s:s.lower() if type(s) == str else s)


# In[87]:


def modify_data(row, return_type='state_province_country'):
    if return_type == 'population_type':
        value = row['population_region']
        if '.1' in value:
            return 'male'
        elif '.2' in value:
            return 'female'
        else:
            return 'total'
    if return_type == 'state_province_country':
        value = row['population_region']
        if '.1' in value:
            return value.replace('.1', '')
        elif '.2' in value:
            return value.replace('.2', '')
        else:
            return value


# In[88]:


population_dataset['population_type'] = population_dataset.apply(
    lambda row: modify_data(row, return_type="population_type"), axis=1
)


# In[89]:


population_dataset['population_region'] = population_dataset.apply(
    lambda row: modify_data(row, return_type="state_province_country"), axis=1
)


# In[90]:


population_dataset.to_csv(population_target_filename, sep=',', encoding='utf-8', index = False)


# # Covid data 

# In[25]:


covid_df = pd.read_csv(covid_source_filename)
covid_df= covid_df.applymap(lambda s:s.lower() if type(s) == str else s)


# In[26]:


def calculate_new_rows(row, _type="IFR", region='state'):
    if _type == "IFR":
        if region == 'state':
            try:
                return round(row['deaths']/row['active'] * 100, 2)
            except ZeroDivisionError:
                return 0
        else:
            try:
                return round(row['district_deaths']/row['district_active'] * 100, 2)
            except ZeroDivisionError:
                return 0
    elif _type == "CFR":
        if region == 'state':
            try:
                return round(row['deaths']/row['cases'] * 100, 2)
            except ZeroDivisionError:
                return 0
        else:
            try:
                return round(row['district_deaths']/row['district_cases'] * 100, 2)
            except ZeroDivisionError:
                return 0
    elif _type == "CFR_CURRENT":
        if region == 'state':
            try:
                return round(row['deaths']/(row['deaths'] + row['cured']) * 100, 2)
            except ZeroDivisionError:
                return 0
        else:
            try:
                return round(row['district_deaths']/(row['district_deaths'] + row['district_cured']) * 100, 2)
            except ZeroDivisionError:
                return 0


# # IFR per state and district
# ### Infection fatality ratio (IFR, in %) = Number of deaths from disease/Number of infected individuals x 100 
# 

# In[27]:


covid_df['state_IFR'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="IFR"), axis=1
)
covid_df['district_IFR'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="IFR", region='district'), axis=1
)


# # CFR per state and district
# ## Case fatality ratio (IFR, in %) = Number of deaths from disease/Number of confirmed cases x 100

# In[28]:


covid_df['state_CFR'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="CFR"), axis=1
)
covid_df['district_CFR'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="CFR", region='district'), axis=1
)


# ## CFR per state and district during an ongoing epidemic
# ## Infection fatality ratio (IFR, in %) = Number of deaths from disease/(Number of deaths from disease + Number of recovered cases) x 100

# In[29]:


covid_df['state_CFR_CURRENT'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="CFR_CURRENT"), axis=1
)
covid_df['district_CFR_CURRENT'] = covid_df.apply(
    lambda row: calculate_new_rows(row, _type="CFR_CURRENT", region='district'), axis=1
)


# In[30]:


covid_df.to_csv(covid_target_filename, sep=',', encoding='utf-8', index = False)


# # Transformation based on all source Dataset

# In[124]:


covid_df = pd.read_csv(covid_target_filename)
population_df = pd.read_csv(population_target_filename)
gsdp_df = pd.read_csv(gsdp_target_filename)
unemployment_df = pd.read_csv(unemployment_target_filename)


# In[92]:


#covid_df


# In[94]:


#population_df


# In[58]:


#gsdp_df


# In[57]:


#unemployment_df.head(50)


# In[125]:


uemp_gsdp_df = gsdp_df.merge(
    unemployment_df,how='left',
    left_on=['gsdp_year', 'gsdp_region'],
    right_on=['unemployment_year', 'unemployment_region']
).fillna(0).astype(
    pd.concat([gsdp_df.dtypes,unemployment_df.dtypes])
)


# In[126]:


#uemp_gsdp_df


# In[127]:


pop_uemp_gsdp_df = population_df.merge(
    uemp_gsdp_df,how='left', 
    left_on=['population_year', 'population_region'],
    right_on=['gsdp_year', 'gsdp_region']
).fillna(0).astype(
    pd.concat([population_df.dtypes,uemp_gsdp_df.dtypes])
)


# In[119]:


#pop_uemp_gsdp_df


# In[128]:


covid_pop_uemp_gsdp_df = covid_df.merge(
    pop_uemp_gsdp_df,how='left', left_on=['state_name'], right_on=['population_region']
).fillna(0).astype(
    pd.concat([covid_df.dtypes,pop_uemp_gsdp_df.dtypes])
)


# In[129]:


#covid_pop_uemp_gsdp_df.head(60)


# In[130]:


covid_pop_uemp_gsdp_df.to_csv(
    covid_pop_uemp_gsdp_output_file_name, sep=',', encoding='utf-8', index = False
)


# In[132]:


covid_pop_uemp_gsdp_df


# In[ ]:




