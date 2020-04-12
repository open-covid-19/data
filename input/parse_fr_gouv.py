#!/usr/bin/env python

import sys
from datetime import datetime
from pandas import DataFrame, isna
from covid_io import read_file
from utils import cumsum_table, merge_previous, dataframe_output


# FR_ARA,FR,France,ARA,Auvergne-Rhône-Alpes,ARA,45.447100,4.385300,
# FR_BFC,FR,France,BFC,Bourgogne-Franche-Comté,BFC,47.280500,4.999400,
# FR_BRE,FR,France,BRE,Brittany,BRE,48.202000,-2.932600,
# FR_COR,FR,France,COR,Corsica,COR,42.039600,9.012900,
# FR_CVL,FR,France,CVL,Centre-Val de Loire,CVL,47.751600,1.675100,
# FR_GES,FR,France,GES,Grand Est,GES,48.699800,6.187800,
# FR_GF,FR,France,GF,French Guiana,GF,3.933900,-53.125800,
# FR_GUA,FR,France,GUA,Guadeloupe,GUA,16.265000,-61.551000,
# FR_HDF,FR,France,HDF,Hauts-de-France,HDF,50.480100,2.793700,
# FR_IDF,FR,France,IDF,Île-de-France,IDF,48.849900,2.637000,
# FR_LRE,FR,France,LRE,La Réunion,LRE,-21.115100,55.536400,
# FR_MAY,FR,France,MAY,Mayotte,MAY,-12.827500,45.166200,
# FR_MQ,FR,France,MQ,Martinique,MQ,14.641500,-61.024200,
# FR_NAQ,FR,France,NAQ,Nouvelle-Aquitaine,NAQ,45.708700,0.626900,
# FR_NOR,FR,France,NOR,Normandy,NOR,48.879900,0.171300,
# FR_OCC,FR,France,OCC,Occitanie,OCC,43.892700,3.282800,
# FR_PAC,FR,France,PAC,Provence-Alpes-Côte d'Azur,PAC,43.935200,6.067900,
# FR_PDL,FR,France,PDL,Pays de la Loire,PDL,47.763300,-0.330000,


# Read the ISO mappings for department -> region
iso = read_file(sys.argv[1], table_index=2, header=True)
region_column = [col for col in iso.columns if 'region' in col.lower()][0]
dep_map = {idx[3:]: code for idx, code in zip(iso['Code'], iso[region_column])}

# Add a few extra departments not in agreement with Wikipedia
dep_map['971'] = 'GUA'
dep_map['972'] = 'MQ'
dep_map['973'] = 'GF'
dep_map['974'] = 'LRE'
dep_map['976'] = 'MAY'

# Read the data from data.gouv.fr
confirmed = read_file(sys.argv[2], sep=';').rename(columns={
    'jour': 'Date',
    'dep': 'RegionCode',
    'clage_covid': 'AgeGroup',
    'nb_test': 'Tested',
    'nb_pos': 'Confirmed'
})
deaths = read_file(sys.argv[3], sep=';').rename(columns={
    'jour': 'Date',
    'dep': 'RegionCode',
    'incid_dc': 'Deaths'
})

# Read the previous data to match
previous = read_file(sys.argv[4])

# Confirmed cases are split into age groups, add up all groups
keys = ['RegionCode', 'Date']
confirmed = confirmed.set_index(keys)
confirmed = confirmed.groupby(['Date', 'RegionCode']).sum()
confirmed = confirmed.reset_index()

# Join the confirmed and deaths tables
data = confirmed.merge(deaths, how='outer')

# Map the department to the region
data['RegionCode'] = data['RegionCode'].apply(lambda dep: dep_map.get(dep))

# Data is new cases, perform the cumsum to get total
data = cumsum_table(data.dropna(subset=keys).set_index(keys)).reset_index()

# Merge with the prior data
previous = previous[previous['CountryCode'] == 'FR']
previous = previous[~previous['RegionCode'].isna()]
data = merge_previous(data, previous, ['Date', 'RegionCode'])

# New data is incomplete for Confirmed, so use the prior data when available
data = data.set_index(keys)
previous = previous.set_index(keys).dropna()
data.loc[previous.index] = previous
# print(data.tail(50))

# Output the results
dataframe_output(data.reset_index(), 'FR')
